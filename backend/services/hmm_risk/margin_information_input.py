"""Explicitly bind shared financing identity without changing the frozen release.

This is a file-only input adapter, not a feature, model or eligibility policy.
The old 20D consumer keeps its own pinned security authority unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .contracts import StateModelSetError
from .monthly_release_input import _Files, _manifest_pins, _reference
from .security_identity import (
    SHARED_SECURITY_IDENTITY_SCHEMA,
    SecuritySourceIdentityManifest,
    load_security_source_identity_manifest,
)

MARGIN_SOURCE = "market.margin_detail"
MARGIN_PATH = "components/factor_h5_static_candidate_v2/margin_detail.h5"
_OLD_AUTHORITY = "components/factor_h5_static_candidate_v2/security_source_identity.json"
_PACKAGE_SCHEMA = "aistock_shared_security_source_identity_package_v1"


def _invalid(message: str) -> StateModelSetError:
    return StateModelSetError(f"hmm_risk_margin_source_binding_invalid: {message}")


@dataclass(frozen=True)
class MarginSourceBinding:
    margin_path: Path
    identity: SecuritySourceIdentityManifest
    dataset_manifest_sha256: str
    package_manifest_sha256: str
    _files: _Files

    def canonical_code(self, source_ts_code: str, trade_date: date) -> str:
        """Invert only this source's effective aliases, rejecting wrong-side codes."""
        if type(trade_date) is not date:
            raise _invalid("financing observation date must be a date")
        aliases = [row for row in self.identity.rows if row.source_dataset == MARGIN_SOURCE]
        candidates = [row for row in aliases if row.source_ts_code == source_ts_code]
        if candidates:
            active = [row for row in candidates if row.effective_start <= trade_date <= row.effective_end]
            if len(active) != 1:
                raise _invalid(f"unresolved/ambiguous financing alias: {source_ts_code}/{trade_date}")
            canonical = active[0].canonical_ts_code
        else:
            canonical = source_ts_code
        resolved = self.identity.resolve(canonical, trade_date, MARGIN_SOURCE)
        if resolved.source_ts_code != source_ts_code:
            raise _invalid(f"financing code outside its effective identity: {source_ts_code}/{trade_date}")
        return canonical

    def evidence(self) -> dict[str, Any]:
        return {
            "dataset_manifest_sha256": self.dataset_manifest_sha256,
            "package_manifest_sha256": self.package_manifest_sha256,
            "source_dataset": MARGIN_SOURCE,
            "authority": self.identity.evidence(),
            "input_file_sha256": {str(path): digest for path, digest in self._files.hashes.items()},
            "identity_is_not_financing_eligibility": True,
            "historical_first_publication_time": "UNKNOWN",
        }

    def unchanged(self) -> None:
        self._files.unchanged()


def load_margin_source_binding(
    *,
    dataset_root: Path,
    expected_dataset_manifest_sha256: str,
    package_root: Path,
    expected_package_manifest_sha256: str,
) -> MarginSourceBinding:
    """Check both explicit identities and all consumed bytes; no defaults/search/DB."""
    files = _Files(dataset_root)
    manifest = files.read(files.path("qe_dataset_manifest.json"))
    pins = _manifest_pins(manifest)
    if manifest.get("schema_version") != "qe_dataset_manifest_v1" or (
        manifest.get("dataset_manifest_sha256") != expected_dataset_manifest_sha256
    ):
        raise _invalid("dataset manifest identity/schema differs")
    files.digest(files.path("qe_dataset_manifest.json"))
    if MARGIN_PATH not in pins or _OLD_AUTHORITY not in pins:
        raise _invalid("release omits financing or original identity pin")
    margin = files.verify(pins[MARGIN_PATH])
    old_authority = files.verify(pins[_OLD_AUTHORITY])
    package_path = files.external(package_root / "manifest.json")
    if files.digest(package_path) != expected_package_manifest_sha256:
        raise _invalid("shared package manifest SHA256 differs")
    package = files.read(package_path)
    if (
        package.get("schema_version") != _PACKAGE_SCHEMA
        or package.get("predecessor_manifest_identity") != expected_dataset_manifest_sha256
        or package.get("predecessor_data_sha256") != files.digest(margin)
        or package.get("data_files_reused_unchanged") is not True
        or not isinstance(package.get("files"), dict)
        or set(package["files"]) != {"security_source_identity.json", "margin_source_identity_readback.json"}
    ):
        raise _invalid("shared package does not bind this frozen release/financing file")
    assets = {}
    for name, pin in package["files"].items():
        if not isinstance(pin, dict) or set(pin) != {"sha256", "size"}:
            raise _invalid(f"shared asset pin schema differs: {name}")
        relative, expected, size = _reference({"path": name, **pin})
        path = files.external(package_root / relative)
        if path.stat().st_size != size or files.digest(path) != expected:
            raise _invalid(f"shared package asset differs: {name}")
        assets[name] = files.read(path)
    report = assets["margin_source_identity_readback.json"]
    authority_evidence = report.get("identity", {})
    if not isinstance(authority_evidence, dict):
        raise _invalid("shared source readback omits identity evidence")
    identity = load_security_source_identity_manifest(
        package_root / "security_source_identity.json",
        expected_sha256=authority_evidence.get("canonical_sha256", ""),
    )
    if (
        identity.schema_version != SHARED_SECURITY_IDENTITY_SCHEMA
        or report.get("schema_version") != "aistock_margin_source_identity_readback_v1"
        or report.get("status") != "PASS"
        or report.get("predecessor_bytes_unchanged") is not True
        or report.get("predecessor_manifest_identity") != expected_dataset_manifest_sha256
        or report.get("predecessor_manifest_file_sha256") != files.digest(files.path("qe_dataset_manifest.json"))
        or report.get("predecessor_identity_sha256") != files.digest(old_authority)
        or report.get("margin_detail_sha256") != files.digest(margin)
        or authority_evidence.get("file_sha256") != files.digest(package_root / "security_source_identity.json")
        or package.get("authority_version") != identity.manifest_version
    ):
        raise _invalid("shared source readback identities/status differ")
    aliases = [row for row in identity.rows if row.source_dataset == MARGIN_SOURCE]
    if not aliases:
        raise _invalid("shared authority has no financing-source alias; no cross-source substitution")
    for index, row in enumerate(aliases):
        for previous in aliases[:index]:
            if (
                row.source_ts_code == previous.source_ts_code
                and row.effective_start <= previous.effective_end
                and previous.effective_start <= row.effective_end
            ):
                raise _invalid("shared financing aliases have ambiguous inverse identity")
    files.unchanged()
    return MarginSourceBinding(
        margin, identity, expected_dataset_manifest_sha256, expected_package_manifest_sha256, files
    )
