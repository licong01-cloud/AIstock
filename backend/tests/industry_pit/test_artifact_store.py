from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from backend.services.industry_pit import artifact_store
from backend.services.industry_pit.artifact_store import (
    read_candidate_bundle,
    require_repo_external_root,
    write_candidate_bundle,
)
from backend.services.industry_pit.contracts import (
    CLASSIFICATION_CANDIDATE_SCHEMA,
    INDEX_MEMBERSHIP_CANDIDATE_SCHEMA,
    AuthorityReceipt,
    AuthorityType,
    IndustryPitContractError,
    KnowledgeTimePolicy,
    ResearchBasis,
    TaxonomyIdentity,
    UnavailableReason,
    make_candidate_interval,
)
from backend.services.dataset_release.canonical import canonical_json_bytes, digest_named_fields, sha256_hex


SOURCE = "a" * 64
DENOMINATOR = "d" * 64
IDENTITY = TaxonomyIdentity("220000", "基础化工", "220300", "化学制品", "220315", "食品及饲料添加剂")


def _receipt(authority: AuthorityType) -> AuthorityReceipt:
    return AuthorityReceipt(
        authority_type=authority,
        authority_schema=(
            CLASSIFICATION_CANDIDATE_SCHEMA
            if authority is AuthorityType.CLASSIFICATION
            else INDEX_MEMBERSHIP_CANDIDATE_SCHEMA
        ),
        authority_version=f"{authority.value}_v1",
        taxonomy_contract_id="sw2021_classification_catalog_v1",
        taxonomy_version="SW2021",
        knowledge_time_policy=KnowledgeTimePolicy.CAUSAL_DAILY_NEXT_TRADE,
        research_basis=ResearchBasis.AS_PUBLISHED_PIT,
        source_ids=("test:source",),
        source_hashes=(SOURCE,),
        frozen_denominator=1,
        denominator_digest=DENOMINATOR,
    )


def _row(receipt: AuthorityReceipt, *, resolved: bool):
    return make_candidate_interval(
        canonical_symbol="300741.SZ",
        authority_type=receipt.authority_type,
        taxonomy_contract_id=receipt.taxonomy_contract_id,
        taxonomy_version=receipt.taxonomy_version,
        authority_receipt_hash=receipt.receipt_hash,
        valid_from=date(2021, 7, 30),
        valid_to_exclusive=None,
        eligible_from=date(2021, 7, 30),
        eligible_to_exclusive=date(2021, 8, 3),
        causal_use_from=date(2021, 8, 2),
        causal_use_to_exclusive=None,
        known_from=date(2021, 8, 2),
        source_effective_field=(
            "计入日期" if receipt.authority_type is AuthorityType.CLASSIFICATION else "membership_enter_date"
        ),
        source_last_updated_at=None,
        research_basis=receipt.research_basis,
        non_as_known_taxonomy=False,
        identity=IDENTITY if resolved else None,
        authority_identity=(
            {
                "classification_l1_code": IDENTITY.l1_code,
                "classification_l2_code": IDENTITY.l2_code,
                "classification_l3_code": IDENTITY.l3_code,
            }
            if resolved
            else {}
        ),
        unavailable_reason=None if resolved else UnavailableReason.MEMBERSHIP_BOUNDARY_UNAVAILABLE,
        source_ids=receipt.source_ids,
        source_hashes=(SOURCE,),
        lineage_hashes=(SOURCE,),
    )


def _catalog():
    payload = {
        "schema_version": "sw2021_taxonomy_catalog_v1",
        "contract_id": "sw2021_classification_catalog_v1",
        "version": "SW2021",
        "source_sha256": SOURCE,
        "identities": {"220315": IDENTITY.as_dict()},
    }
    return {**payload, "catalog_hash": sha256_hex(canonical_json_bytes(payload))}


def _report():
    payload = {
        "schema_version": "industry_pit_full_denominator_preflight_v1",
        "denominator_digest": DENOMINATOR,
        "total_opportunities": 1,
        "classification": {"resolved": 1, "unavailable": 0},
        "index_membership": {"resolved": 0, "unavailable": 1},
        "unavailable_by_reason": {
            "sw_industry_index_membership_pit:membership_boundary_unavailable": 1
        },
        "closure": {
            "classification_resolved_plus_unavailable": 1,
            "index_resolved_plus_unavailable": 1,
            "expected_denominator": 1,
            "passed": True,
        },
    }
    return {
        **payload,
        "canonical_hash": digest_named_fields(
            "industry_pit_full_denominator_preflight_v1", payload
        ),
    }


def test_writer_readback_uses_one_schema_and_preserves_separate_hashes(tmp_path: Path) -> None:
    forbidden = tmp_path / "repo"
    forbidden.mkdir()
    target = tmp_path / "artifacts" / "candidate"
    classification_receipt = _receipt(AuthorityType.CLASSIFICATION)
    index_receipt = _receipt(AuthorityType.INDEX_MEMBERSHIP)
    readback = write_candidate_bundle(
        artifact_root=target,
        forbidden_roots=(forbidden,),
        taxonomy_catalog=_catalog(),
        classification_receipt=classification_receipt,
        index_membership_receipt=index_receipt,
        classification_intervals=(_row(classification_receipt, resolved=True),),
        index_membership_intervals=(_row(index_receipt, resolved=False),),
        preflight_report=_report(),
        producer_commit="1" * 40,
        producer_tree="2" * 40,
    )
    assert readback.artifact_root == target.resolve()
    assert readback.manifest["classification_candidate_hash"] != readback.manifest[
        "index_membership_candidate_hash"
    ]
    assert readback.classification_receipt.receipt_hash == classification_receipt.receipt_hash
    assert readback.index_membership_receipt.receipt_hash == index_receipt.receipt_hash


@pytest.mark.parametrize(
    ("chunk_bytes", "chunk_rows"),
    ((1, 100), (1024 * 1024, 2)),
    ids=("byte-bound", "row-bound"),
)
def test_jsonl_writer_uses_bounded_multistage_sort_and_preserves_canonical_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    chunk_bytes: int,
    chunk_rows: int,
) -> None:
    monkeypatch.setattr(artifact_store, "_JSONL_SORT_CHUNK_BYTES", chunk_bytes)
    monkeypatch.setattr(artifact_store, "_JSONL_SORT_CHUNK_ROWS", chunk_rows)
    monkeypatch.setattr(artifact_store, "_JSONL_MERGE_FAN_IN", 2)
    rows = [
        {"symbol": "300858.SZ", "rank": 4},
        {"symbol": "300741.SZ", "rank": 2},
        {"symbol": "603020.SH", "rank": 3},
        {"symbol": "300741.SZ", "rank": 1},
        {"symbol": "605077.SH", "rank": 5},
        {"symbol": "300741.SZ", "rank": 1},
    ]
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"

    first_observation = artifact_store._write_jsonl(first, iter(rows))
    second_observation = artifact_store._write_jsonl(second, iter(reversed(rows)))

    expected = b"".join(
        row + b"\n" for row in sorted(canonical_json_bytes(dict(value)) for value in rows)
    )
    assert first.read_bytes() == expected
    assert second.read_bytes() == expected
    assert first_observation == second_observation
    assert first_observation.row_count == len(rows)
    assert first_observation.size_bytes == len(expected)
    assert first_observation.sha256 == sha256_hex(expected)


def test_jsonl_writer_empty_input_preserves_legacy_empty_file_identity(tmp_path: Path) -> None:
    target = tmp_path / "empty.jsonl"

    observation = artifact_store._write_jsonl(target, ())

    assert target.read_bytes() == b""
    assert observation.row_count == 0
    assert observation.size_bytes == 0
    assert observation.sha256 == sha256_hex(b"")
    assert list(tmp_path.glob(".empty.jsonl.sort-*")) == []


def test_bundle_writer_and_readback_do_not_read_candidate_files_whole(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "artifacts" / "candidate"
    classification_receipt = _receipt(AuthorityType.CLASSIFICATION)
    index_receipt = _receipt(AuthorityType.INDEX_MEMBERSHIP)
    original_read_bytes = Path.read_bytes

    def reject_candidate_read_bytes(path: Path) -> bytes:
        inside_staging = any(
            parent.name.startswith(f".{target.name}.tmp-") for parent in path.parents
        )
        if path.suffix in {".json", ".jsonl"} and (
            path == target or target in path.parents or inside_staging
        ):
            raise AssertionError(f"candidate artifact used Path.read_bytes(): {path}")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", reject_candidate_read_bytes)
    readback = write_candidate_bundle(
        artifact_root=target,
        forbidden_roots=(tmp_path / "repo",),
        taxonomy_catalog=_catalog(),
        classification_receipt=classification_receipt,
        index_membership_receipt=index_receipt,
        classification_intervals=(_row(classification_receipt, resolved=True),),
        index_membership_intervals=(_row(index_receipt, resolved=False),),
        preflight_report=_report(),
        producer_commit="1" * 40,
        producer_tree="2" * 40,
    )

    assert readback.artifact_root == target.resolve()
    assert readback.manifest["files"]["classification_candidate.jsonl"]["row_count"] == 1


def test_jsonl_writer_cleans_private_sort_workspace_after_merge_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(artifact_store, "_JSONL_SORT_CHUNK_ROWS", 1)
    monkeypatch.setattr(artifact_store, "_JSONL_MERGE_FAN_IN", 2)

    def fail_merge(paths: object, output: object) -> int:
        raise OSError("synthetic merge failure")

    monkeypatch.setattr(artifact_store, "_merge_sorted_files", fail_merge)
    target = tmp_path / "candidate.jsonl"
    with pytest.raises(OSError, match="synthetic merge failure"):
        artifact_store._write_jsonl(
            target,
            ({"rank": rank} for rank in (3, 2, 1)),
        )

    assert not target.exists()
    assert list(tmp_path.glob(".candidate.jsonl.sort-*")) == []


def test_jsonl_writer_withdraws_final_file_when_sort_workspace_cleanup_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_rmtree = artifact_store.shutil.rmtree

    def fail_sort_cleanup(path: object, *args: object, **kwargs: object) -> None:
        if Path(path).name.startswith(".candidate.jsonl.sort-"):
            raise OSError("synthetic sort cleanup failure")
        original_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(artifact_store.shutil, "rmtree", fail_sort_cleanup)
    target = tmp_path / "candidate.jsonl"
    with pytest.raises(OSError, match="synthetic sort cleanup failure"):
        artifact_store._write_jsonl(target, ({"rank": rank} for rank in (2, 1)))

    assert not target.exists()
    leftovers = list(tmp_path.glob(".candidate.jsonl.sort-*"))
    assert len(leftovers) == 1
    monkeypatch.setattr(artifact_store.shutil, "rmtree", original_rmtree)
    original_rmtree(leftovers[0])


def test_tamper_is_typed_writer_readback_hash_mismatch(tmp_path: Path) -> None:
    target = tmp_path / "artifacts" / "candidate"
    classification_receipt = _receipt(AuthorityType.CLASSIFICATION)
    index_receipt = _receipt(AuthorityType.INDEX_MEMBERSHIP)
    write_candidate_bundle(
        artifact_root=target,
        forbidden_roots=(tmp_path / "repo",),
        taxonomy_catalog=_catalog(),
        classification_receipt=classification_receipt,
        index_membership_receipt=index_receipt,
        classification_intervals=(_row(classification_receipt, resolved=True),),
        index_membership_intervals=(_row(index_receipt, resolved=False),),
        preflight_report=_report(),
        producer_commit="1" * 40,
        producer_tree="2" * 40,
    )
    with (target / "classification_candidate.jsonl").open("ab") as handle:
        handle.write(b" ")
    with pytest.raises(IndustryPitContractError, match="writer_readback_hash_mismatch"):
        read_candidate_bundle(artifact_root=target, forbidden_roots=(tmp_path / "repo",))


def test_readback_rejects_rehashed_internal_catalog_and_preflight_tamper(tmp_path: Path) -> None:
    target = tmp_path / "artifacts" / "candidate"
    classification_receipt = _receipt(AuthorityType.CLASSIFICATION)
    index_receipt = _receipt(AuthorityType.INDEX_MEMBERSHIP)
    write_candidate_bundle(
        artifact_root=target,
        forbidden_roots=(tmp_path / "repo",),
        taxonomy_catalog=_catalog(),
        classification_receipt=classification_receipt,
        index_membership_receipt=index_receipt,
        classification_intervals=(_row(classification_receipt, resolved=True),),
        index_membership_intervals=(_row(index_receipt, resolved=False),),
        preflight_report=_report(),
        producer_commit="1" * 40,
        producer_tree="2" * 40,
    )
    catalog_path = target / "taxonomy_catalog.json"
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    catalog["version"] = "TAMPERED"
    catalog_path.write_bytes(canonical_json_bytes(catalog) + b"\n")
    manifest_path = target / "candidate_bundle_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"]["taxonomy_catalog.json"]["sha256"] = sha256_hex(catalog_path.read_bytes())
    manifest["files"]["taxonomy_catalog.json"]["size_bytes"] = catalog_path.stat().st_size
    manifest_path.write_bytes(canonical_json_bytes(manifest) + b"\n")
    with pytest.raises(IndustryPitContractError, match="taxonomy catalog hash mismatch"):
        read_candidate_bundle(artifact_root=target, forbidden_roots=(tmp_path / "repo",))


def test_writer_rejects_cross_authority_receipt_and_preflight_mismatch(tmp_path: Path) -> None:
    classification_receipt = _receipt(AuthorityType.CLASSIFICATION)
    index_receipt = _receipt(AuthorityType.INDEX_MEMBERSHIP)
    report = dict(_report())
    report_payload = {key: value for key, value in report.items() if key != "canonical_hash"}
    report_payload["denominator_digest"] = "f" * 64
    report = {
        **report_payload,
        "canonical_hash": digest_named_fields(
            "industry_pit_full_denominator_preflight_v1", report_payload
        ),
    }
    with pytest.raises(IndustryPitContractError, match="receipt denominator mismatch"):
        write_candidate_bundle(
            artifact_root=tmp_path / "artifacts" / "candidate",
            forbidden_roots=(tmp_path / "repo",),
            taxonomy_catalog=_catalog(),
            classification_receipt=classification_receipt,
            index_membership_receipt=index_receipt,
            classification_intervals=(_row(classification_receipt, resolved=True),),
            index_membership_intervals=(_row(index_receipt, resolved=False),),
            preflight_report=report,
            producer_commit="1" * 40,
            producer_tree="2" * 40,
        )


def test_writer_refuses_repo_root_overwrite_and_non_finite_json(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    with pytest.raises(IndustryPitContractError, match="repo-external"):
        require_repo_external_root(repo / "candidate", forbidden_roots=(repo,))

    classification_receipt = _receipt(AuthorityType.CLASSIFICATION)
    index_receipt = _receipt(AuthorityType.INDEX_MEMBERSHIP)
    report = dict(_report())
    report["invalid"] = float("nan")
    with pytest.raises(Exception, match="non-finite"):
        write_candidate_bundle(
            artifact_root=tmp_path / "artifacts" / "nan-candidate",
            forbidden_roots=(repo,),
            taxonomy_catalog=_catalog(),
            classification_receipt=classification_receipt,
            index_membership_receipt=index_receipt,
            classification_intervals=(_row(classification_receipt, resolved=True),),
            index_membership_intervals=(_row(index_receipt, resolved=False),),
            preflight_report=report,
            producer_commit="1" * 40,
            producer_tree="2" * 40,
        )
