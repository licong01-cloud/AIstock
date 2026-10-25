"""Source-reviewed L2 asset admissions, shared by compose and isolated workers.

Requests select a version; they cannot supply an allowlist or approve a hash.
Append a new record only after reviewing an actual matching producer delivery.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from types import MappingProxyType

APPROVED_RISK_SHA256 = "5e6a2784a304b2a34cb8cbf6859aab0a9c154a29ba5fac246ff35eebcd9ca0e5"
LEGACY_MANIFEST = "522e8e35896e85fc9289ca2ceca1cd6127f4c7978a72766887c0429dc9b7f410"
LEGACY_GENERATION = "20260930-monthly-v2-unified-correction-4ab0b48642c3"
ADMISSIONS = MappingProxyType({
    "l2-risk-522-v1": MappingProxyType({
        "risk_sha256": APPROVED_RISK_SHA256,
        "release_identity_sha256": "be10b86816c0bd3675d73821faae0b5e9b6decf51c329f5a664605718be3bf4b",
        "dataset_binding_sha256": "f87e12b7dec5b432952c47e2db8c91defc124ccdef1223a9cbd3cefed85d21ec",
        "authority_canonical_sha256": "344dddef0d729ff231d697eb70d0d362195c3e2c2b30597c51f4e5d182b00e4c",
    }),
})


class L2AdmissionError(ValueError):
    def __init__(self, code, detail=""):
        self.reason_code = code
        super().__init__(f"{code}: {detail}")


def canonical_sha(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def physical_binding(binding):
    result = deepcopy(binding)
    result.get("selection_pins", {}).pop("coverage_receipt_sha256", None)
    return result


def validate_admission(manifest):
    """Reject unreviewed assets and cross-release rebinding before asset IO."""
    risk_sha = manifest.get("assets", {}).get("risk", {}).get("sha256")
    version = manifest.get("asset_admission_version")
    if version is None:
        if risk_sha != APPROVED_RISK_SHA256:
            raise L2AdmissionError("QE_L2_UNAPPROVED_RISK_ASSET")
        release = manifest.get("release_identity", {})
        if (release.get("dataset_manifest_sha256") != LEGACY_MANIFEST
                or release.get("frozen_release_generation") != LEGACY_GENERATION):
            raise L2AdmissionError("QE_L2_LEGACY_ASSET_REBIND_FORBIDDEN")
        return "legacy-522"
    if not isinstance(version, str) or version not in ADMISSIONS:
        raise L2AdmissionError("QE_L2_ASSET_ADMISSION_UNKNOWN")
    observed = {
        "risk_sha256": risk_sha,
        "release_identity_sha256": canonical_sha(manifest.get("release_identity")),
        "dataset_binding_sha256": canonical_sha(physical_binding(manifest.get("dataset_binding", {}))),
        "authority_canonical_sha256": manifest.get("authority_canonical_sha256"),
    }
    if observed != dict(ADMISSIONS[version]):
        raise L2AdmissionError("QE_L2_ASSET_ADMISSION_MISMATCH", version)
    return version
