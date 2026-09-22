from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.runtime_release_registration import (
    RUNTIME_RELEASE_REGISTRY_DIRECTORY,
    RuntimeReleaseRegistrationError,
    ensure_runtime_release_registration,
    read_runtime_release_registration,
)


def _release(parent: Path, name: str = "20260930-candidate") -> tuple[Path, str]:
    root = parent / name
    root.mkdir()
    manifest = {
        "schema_version": "qe_dataset_manifest_v1",
        "release_id": "qe_hmm_full_v2_20260930",
        "cutoff_trade_date": "2026-09-30",
    }
    identity = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    manifest["dataset_manifest_sha256"] = identity
    (root / "qe_dataset_manifest.json").write_bytes(
        canonical_json_bytes(manifest) + b"\n"
    )
    return root, identity


def test_registration_is_create_exclusive_and_idempotent(tmp_path: Path) -> None:
    parent = tmp_path / "releases"
    parent.mkdir()
    root, identity = _release(parent)

    first = ensure_runtime_release_registration(
        allowed_parent=parent,
        candidate_root=root,
        dataset_manifest_sha256=identity,
    )
    second = ensure_runtime_release_registration(
        allowed_parent=parent,
        candidate_root=root,
        dataset_manifest_sha256=identity,
    )
    readback = read_runtime_release_registration(
        allowed_parent=parent,
        candidate_root=root,
        dataset_manifest_sha256=identity,
    )

    assert first == second == readback
    entry = parent / first.relative_path
    payload = json.loads(entry.read_text(encoding="utf-8"))
    assert payload["dataset_manifest_sha256"] == identity
    assert payload["candidate_root_name"] == root.name
    assert first.sha256 == hashlib.sha256(entry.read_bytes()).hexdigest()


def test_registration_rejects_existing_identity_drift(tmp_path: Path) -> None:
    parent = tmp_path / "releases"
    parent.mkdir()
    root, identity = _release(parent)
    created = ensure_runtime_release_registration(
        allowed_parent=parent,
        candidate_root=root,
        dataset_manifest_sha256=identity,
    )
    (parent / created.relative_path).write_bytes(b"{}\n")

    with pytest.raises(RuntimeReleaseRegistrationError, match="differs"):
        ensure_runtime_release_registration(
            allowed_parent=parent,
            candidate_root=root,
            dataset_manifest_sha256=identity,
        )


@pytest.mark.skipif(os.name == "nt", reason="symlink creation is not portable on Windows")
def test_registration_rejects_linked_registry(tmp_path: Path) -> None:
    parent = tmp_path / "releases"
    parent.mkdir()
    external = tmp_path / "external"
    external.mkdir()
    (parent / RUNTIME_RELEASE_REGISTRY_DIRECTORY).symlink_to(
        external,
        target_is_directory=True,
    )
    root, identity = _release(parent)

    with pytest.raises(RuntimeReleaseRegistrationError, match="plain directory"):
        ensure_runtime_release_registration(
            allowed_parent=parent,
            candidate_root=root,
            dataset_manifest_sha256=identity,
        )


@pytest.mark.skipif(os.name == "nt", reason="symlink creation is not portable on Windows")
def test_registration_rejects_linked_candidate(tmp_path: Path) -> None:
    parent = tmp_path / "releases"
    parent.mkdir()
    real_root, identity = _release(tmp_path, name="real-candidate")
    linked_root = parent / "linked-candidate"
    linked_root.symlink_to(real_root, target_is_directory=True)

    with pytest.raises(RuntimeReleaseRegistrationError, match="plain directory"):
        ensure_runtime_release_registration(
            allowed_parent=parent,
            candidate_root=linked_root,
            dataset_manifest_sha256=identity,
        )
