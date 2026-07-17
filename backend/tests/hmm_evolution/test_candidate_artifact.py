from __future__ import annotations

import asyncio
import hashlib
import json

import pytest

from backend.services.hmm_evolution.candidate_artifact import (
    CandidateArtifactParser,
    CandidateArtifactResolver,
)
from backend.services.hmm_evolution.errors import ArtifactManifestInvalidError
from backend.services.hmm_evolution.models import (
    AssetAccessMode,
    AssetTrustLevel,
    CandidateSourceType,
)
from backend.services.hmm_evolution.qe_asset_reader import QEExperimentAssetReader


def _payload_bytes() -> bytes:
    return json.dumps(
        {
            "daily_coefficients": {
                "2026-01-05": {"801010.SI": 1.0, "801020.SI": 1.2},
                "2026-01-06": {"801010.SI": 0.9},
            },
            "stock_sector_map": {
                "000001.SZ": "801010.SI",
                "000002.SZ": "801020.SI",
            },
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def test_candidate_identity_is_content_addressed_not_source_addressed() -> None:
    parser = CandidateArtifactParser()
    payload = _payload_bytes()
    local = parser.preview_bytes(
        payload,
        source_type=CandidateSourceType.CONFIGURED_LOCAL,
        source_ref={"root_alias": "research", "relative_path": "a.json"},
        artifact_uri="configured-local://research/a.json",
    )
    snapshot = parser.preview_bytes(
        payload,
        source_type=CandidateSourceType.EXISTING_SNAPSHOT,
        source_ref={"snapshot_id": "snapshot-1", "artifact_name": "a.json"},
        artifact_uri="snapshot://snapshot-1/a.json",
    )

    assert local.candidate_id == snapshot.candidate_id
    assert local.manifest_hash != snapshot.manifest_hash
    assert local.manifest.coverage.date_count == 2
    assert local.manifest.coverage.sector_count_min == 1
    assert local.manifest.coverage.sector_count_max == 2
    assert local.manifest.coefficient_stats.min == 0.9
    assert local.manifest.coefficient_stats.max == 1.2


@pytest.mark.parametrize(
    "payload",
    [
        b"{}",
        b'{"daily_coefficients":{},"stock_sector_map":{"000001.SZ":"801010.SI"}}',
        b'{"daily_coefficients":{"2026-01-05":{"801010.SI":0}},"stock_sector_map":{"000001.SZ":"801010.SI"}}',
        b'{"daily_coefficients":{"2026-01-06":{"801010.SI":1},"2026-01-05":{"801010.SI":1}},"stock_sector_map":{"000001.SZ":"801010.SI"}}',
        b'{"daily_coefficients":{"2026-01-05":{"801010.SI":1}},"daily_coefficients":{"2026-01-06":{"801010.SI":1}},"stock_sector_map":{"000001.SZ":"801010.SI"}}',
    ],
)
def test_candidate_parser_fails_loud_on_invalid_or_ambiguous_payload(payload: bytes) -> None:
    with pytest.raises(ArtifactManifestInvalidError):
        CandidateArtifactParser().preview_bytes(
            payload,
            source_type=CandidateSourceType.CONFIGURED_LOCAL,
            source_ref={"root_alias": "research", "relative_path": "a.json"},
            artifact_uri="configured-local://research/a.json",
        )


def test_configured_local_resolver_enforces_root_alias_and_containment(tmp_path) -> None:
    root = tmp_path / "coefficients"
    root.mkdir()
    artifact = root / "candidate.json"
    artifact.write_bytes(_payload_bytes())
    resolver = CandidateArtifactResolver(artifact_roots={"research": root})

    preview = resolver.preview_configured_local(
        root_alias="research",
        relative_path="candidate.json",
    )

    assert preview.manifest.artifact_sha256 == hashlib.sha256(_payload_bytes()).hexdigest()
    assert preview.manifest.artifact_uri == "configured-local://research/candidate.json"
    with pytest.raises(ArtifactManifestInvalidError):
        resolver.preview_configured_local(root_alias="missing", relative_path="candidate.json")
    with pytest.raises(Exception, match="path|root|relative"):
        resolver.preview_configured_local(root_alias="research", relative_path="../candidate.json")


class _TrustedQEClient:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    async def list_workspace_files(self, task_id: str, loop_id: str):
        return {
            "catalog_completeness": "complete",
            "files": [
                {
                    "relative_path": "artifacts/hmm.json",
                    "size_bytes": len(self.payload),
                    "sha256": hashlib.sha256(self.payload).hexdigest(),
                    "content_type": "application/json",
                    "trust_level": AssetTrustLevel.TRUSTED_COMPUTATIONAL_INPUT.value,
                    "access_mode": AssetAccessMode.COMPUTATIONAL_INPUT.value,
                    "schema_version": "hmm_sector_coefficients_legacy_v1",
                    "parser_contract": "hmm_sector_coefficients_parser_v1",
                }
            ],
        }

    async def stat_workspace_file(self, task_id: str, loop_id: str, file_path: str):
        return (await self.list_workspace_files(task_id, loop_id))["files"][0]

    async def download_workspace_file_bytes(self, task_id: str, loop_id: str, file_path: str):
        return self.payload


def test_qe_candidate_requires_trusted_manifest_and_parser_receipt() -> None:
    payload = _payload_bytes()
    resolver = CandidateArtifactResolver(
        qe_asset_reader=QEExperimentAssetReader(_TrustedQEClient(payload))
    )

    preview = asyncio.run(
        resolver.preview_qe_experiment(
            task_id="qe_20260706_013235_bbd4",
            loop_name="Loop8",
            relative_path="artifacts/hmm.json",
        )
    )

    assert preview.manifest.source_type is CandidateSourceType.QE_EXPERIMENT
    assert preview.manifest.source_ref["parser_contract"] == "hmm_sector_coefficients_parser_v1"
