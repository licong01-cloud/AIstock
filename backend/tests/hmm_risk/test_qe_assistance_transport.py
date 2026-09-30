from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from backend.services.hmm_risk import qe_assistance_transport as subject


def _binding(path: Path) -> dict:
    content = path.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    return {
        "schema_version": subject.BINDING_SCHEMA_VERSION,
        "artifact_schema_version": subject.SCHEMA_VERSION,
        "local_path": str(path),
        "remote_path": f"/home/lc999/aistock_immutable_assets/hmm_qe_assistance/{digest}/hmm_sector_coefficients.json",
        "file_sha256": digest,
        "canonical_sha256": "a" * 64,
        "size_bytes": len(content),
    }


def test_runtime_binding_verifies_file_and_drops_controller_path(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.json"
    artifact.write_text('{"schema_version":"test"}', encoding="utf-8")
    result = subject.runtime_binding(_binding(artifact))
    assert "local_path" not in result
    assert result["remote_path"].endswith(f"/{result['file_sha256']}/hmm_sector_coefficients.json")


def test_runtime_binding_rejects_changed_file(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.json"
    artifact.write_text("original", encoding="utf-8")
    binding = _binding(artifact)
    artifact.write_text("changed", encoding="utf-8")
    with pytest.raises(subject.QEAssistanceTransportError, match="differs") as exc:
        subject.runtime_binding(binding)
    assert exc.value.reason_code == subject.REASON_LOCAL_HASH_MISMATCH


def test_formal_runtime_binding_rejects_another_self_consistent_artifact(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.json"
    artifact.write_text("other", encoding="utf-8")
    with pytest.raises(subject.QEAssistanceTransportError, match="formal QE-assistance") as exc:
        subject.formal_runtime_binding(_binding(artifact))
    assert exc.value.reason_code == subject.REASON_BINDING_INVALID


@pytest.mark.parametrize(
    "remote_path",
    [
        "relative/hmm_sector_coefficients.json",
        "/home/lc999/qe_workspace/deadbeef/hmm_sector_coefficients.json",
        "/home/lc999/assets/not-the-hash/hmm_sector_coefficients.json",
        "/home/lc999/assets/" + "a" * 64 + "/other.json",
        "/tmp/hmm_qe_assistance/" + "a" * 64 + "/hmm_sector_coefficients.json",
    ],
)
def test_binding_rejects_non_content_addressed_or_workspace_path(tmp_path: Path, remote_path: str) -> None:
    artifact = tmp_path / "artifact.json"
    artifact.write_text("x", encoding="utf-8")
    binding = _binding(artifact)
    binding["remote_path"] = remote_path
    with pytest.raises(subject.QEAssistanceTransportError) as exc:
        subject.runtime_binding(binding)
    assert exc.value.reason_code == subject.REASON_BINDING_INVALID
