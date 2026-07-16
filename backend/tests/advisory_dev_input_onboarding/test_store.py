from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.services.advisory_dev_input_onboarding.contracts import RealDevOnboardingError
from backend.services.advisory_dev_input_onboarding.store import (
    RealDevOnboardingEvidenceStore,
    resolve_package_asset_roots,
)


def test_store_is_atomic_idempotent_and_full_readback(tmp_path: Path, onboarding_request) -> None:
    store = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    first = store.publish(onboarding_request)
    second = store.publish(onboarding_request)
    assert not first.idempotent
    assert second.idempotent
    assert first.ref == second.ref
    assert store.load(first.ref) == onboarding_request
    assert first.path.read_bytes().endswith(b"\n")


def test_store_rejects_tamper_and_identity_collision(tmp_path: Path, onboarding_request) -> None:
    store = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    stored = store.publish(onboarding_request)
    document = json.loads(stored.path.read_text(encoding="utf-8"))
    document["policy_registry_version"] = "tampered"
    stored.path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(RealDevOnboardingError, match="raw hash"):
        store.load(stored.ref)
    with pytest.raises(RealDevOnboardingError, match="identity collision"):
        store.publish(onboarding_request)


def test_store_rejects_repository_internal_root() -> None:
    repository_root = Path(__file__).resolve().parents[3]
    with pytest.raises(RealDevOnboardingError, match="outside the repository"):
        RealDevOnboardingEvidenceStore(root=repository_root / ".onboarding-evidence")


def test_store_rejects_relative_root() -> None:
    with pytest.raises(RealDevOnboardingError, match="must be absolute"):
        RealDevOnboardingEvidenceStore(root=Path("relative-evidence"))


def test_store_rejects_ref_policy_and_path_drift(tmp_path: Path, onboarding_request) -> None:
    store = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    stored = store.publish(onboarding_request)
    with pytest.raises(RealDevOnboardingError, match="store policy"):
        store.load(stored.ref.model_copy(update={"store_policy_hash": "f" * 64}))
    with pytest.raises(RealDevOnboardingError, match="path differs"):
        store.load(stored.ref.model_copy(update={"relative_path": "requests/00/other.json"}))


def test_package_asset_roots_are_explicit_distinct_and_not_created(tmp_path: Path) -> None:
    source = tmp_path / "source-assets"
    source.mkdir()
    target = tmp_path / "target-assets"
    pair = resolve_package_asset_roots(source_root=source, target_root=target)
    assert pair.source_readonly_root == source.resolve()
    assert pair.target_no_replace_root == target.resolve()
    assert not target.exists()
    with pytest.raises(RealDevOnboardingError, match="must be different"):
        resolve_package_asset_roots(source_root=source, target_root=source)
    with pytest.raises(RealDevOnboardingError, match="existing directory"):
        resolve_package_asset_roots(source_root=tmp_path / "missing", target_root=target)
