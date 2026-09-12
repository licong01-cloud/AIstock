from __future__ import annotations

from datetime import date
from decimal import Decimal
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionValueError
from backend.services.position_timing.action_value_corporate_actions import (
    CorporateActionBook,
)
from backend.services.position_timing.contracts import (
    canonical_json_bytes,
    canonical_sha256,
)
from backend.services.position_timing.pattern_research import (
    FACTOR_ACTION_COVERAGE_POLICY_SHA256,
    _rights_issue_request_contract_invalid,
    audit_pattern_factor_action_coverage,
)
from backend.services.position_timing.pattern_rights_issue import (
    RIGHTS_ISSUE_PARTICIPATION_POLICY,
    RIGHTS_ISSUE_PARTICIPATION_POLICY_SHA256,
    combined_corporate_action_source_snapshot,
    freeze_rights_issue_participation_policy,
    open_rights_issue_authority,
    rights_issue_application_audit,
)


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _authority_candidate(tmp_path: Path) -> tuple[Path, str, str, Path]:
    root = tmp_path / "candidate"
    authority_root = root / "components" / "position_timing_source_authority_v1"
    documents = authority_root / "source_documents"
    documents.mkdir(parents=True)
    document = documents / "000001-listing.pdf"
    document.write_bytes(b"official-rights-issue-source")
    document_hash = _sha(document.read_bytes())
    event = {
        "schema_version": "position_timing_rights_issue_event_v1",
        "event_id": "RIGHTS_ISSUE:000001.SZ:2024-01-04",
        "event_key": ["000001.SZ", "RIGHTS_ISSUE", "2024-01-04"],
        "event_type": "RIGHTS_ISSUE",
        "symbol": "000001.SZ",
        "announcement_date": "2024-01-02",
        "disclosure_available_at": "2024-01-01T12:00:00Z",
        "record_date": "2024-01-04",
        "payment_start_date": "2024-01-05",
        "payment_end_date": "2024-01-08",
        "ex_right_date": "2024-01-09",
        "resume_date": "2024-01-09",
        "listing_date": "2024-01-15",
        "entitlement_ratio": 0.3,
        "subscription_price": 5.5,
        "actual_offered_quantity": 300,
        "actual_subscribed_quantity": 290,
        "rounding_rule": "EXCHANGE_CSDC_FRACTIONAL_ENTITLEMENT_RULE_AS_DISCLOSED",
        "issue_success_status": "SUCCESS",
        "source_record_key": "000001-listing",
        "source_url": "https://example.invalid/000001-listing.pdf",
        "source_content_sha256": document_hash,
        "source_document_type": "SHARE_CHANGE_AND_LISTING",
        "source_file_size": document.stat().st_size,
        "source_time_quality": "OFFICIAL_PDF_PIT",
        "captured_at": "2026-09-12T00:00:00Z",
        "versions": [
            {
                "relative_path": "source_documents/000001-listing.pdf",
                "source_content_sha256": document_hash,
                "source_document_type": "SHARE_CHANGE_AND_LISTING",
                "source_file_size": document.stat().st_size,
                "source_record_key": "000001-listing",
                "source_url": "https://example.invalid/000001-listing.pdf",
            }
        ],
    }
    authority = {
        "schema_version": "position_timing_rights_issue_authority_v1",
        "event_count": 1,
        "events": [event],
        "outcomes_read": False,
        "production_database_written": False,
        "runtime_action_performed": False,
        "ddl_performed": False,
    }
    authority["canonical_sha256"] = canonical_sha256(authority)
    authority_path = authority_root / "rights_issue_authority.json"
    authority_path.write_bytes(canonical_json_bytes(authority))
    authority_raw_sha = _sha(authority_path.read_bytes())
    manifest = {
        "schema_version": "qe_dataset_manifest_v1",
        "availability_status": "CANDIDATE_READY",
        "revision": "test-r4",
        "cutoff_trade_date": "2024-01-31",
        "components": {
            "rights_issue_authority": {
                "path": "components/position_timing_source_authority_v1/rights_issue_authority.json",
                "sha256": authority_raw_sha,
                "size": authority_path.stat().st_size,
            }
        },
    }
    manifest["dataset_manifest_sha256"] = canonical_sha256(manifest)
    manifest_path = root / "qe_dataset_manifest.json"
    manifest_path.write_bytes(canonical_json_bytes(manifest))
    return root, _sha(manifest_path.read_bytes()), authority["canonical_sha256"], document


def test_authority_reader_policy_and_application_are_hash_bound(tmp_path: Path):
    root, manifest_sha, authority_sha, _ = _authority_candidate(tmp_path)
    authority = open_rights_issue_authority(
        candidate_root=root,
        expected_candidate_manifest_sha256=manifest_sha,
        expected_authority_canonical_sha256=authority_sha,
    )

    assert authority.candidate_revision == "test-r4"
    assert authority.authority_canonical_sha256 == authority_sha
    assert [event.event_id for event in authority.events] == [
        "RIGHTS_ISSUE:000001.SZ:2024-01-04"
    ]
    event = authority.events[0]
    assert event.entitlement_ratio == Decimal("0.3")
    assert event.subscription_price == Decimal("5.5")
    assert event.listing_date == date(2024, 1, 15)

    policy_path = freeze_rights_issue_participation_policy(
        timing_root=tmp_path / "timing"
    )
    assert _sha(policy_path.read_bytes()) == RIGHTS_ISSUE_PARTICIPATION_POLICY_SHA256
    assert json.loads(policy_path.read_text(encoding="utf-8")) == dict(
        RIGHTS_ISSUE_PARTICIPATION_POLICY
    )

    application = rights_issue_application_audit(
        authority,
        symbols=("000001.SZ",),
        start=date(2024, 1, 2),
        end=date(2024, 1, 31),
    )
    applied = application["applications"][0]
    assert applied["participation_decision"] == "NOT_SUBSCRIBED"
    assert applied["subscribed_quantity"] == 0
    assert applied["subscription_cash_cny"] == "0"
    assert applied["credited_quantity"] == 0
    assert applied["factor_account_quantity_inference"] is False
    assert application["outcomes_read"] is False
    assert application["application_sha256"] == canonical_sha256(
        {key: value for key, value in application.items() if key != "application_sha256"}
    )

    combined = combined_corporate_action_source_snapshot(
        dividend_snapshot_sha256="a" * 64,
        authority=authority,
    )
    assert combined["action_types"] == ("DIVIDEND", "RIGHTS_ISSUE")
    assert combined["snapshot_sha256"] == canonical_sha256(
        {key: value for key, value in combined.items() if key != "snapshot_sha256"}
    )


def test_authority_reader_rejects_changed_official_document(tmp_path: Path):
    root, manifest_sha, authority_sha, document = _authority_candidate(tmp_path)
    document.write_bytes(b"tampered")

    with pytest.raises(
        ActionValueError, match="RIGHTS_ISSUE_SOURCE_VERSION_IDENTITY_MISMATCH"
    ):
        open_rights_issue_authority(
            candidate_root=root,
            expected_candidate_manifest_sha256=manifest_sha,
            expected_authority_canonical_sha256=authority_sha,
        )


def test_rights_issue_binds_factor_change_without_creating_account_shares(
    tmp_path: Path,
):
    root, manifest_sha, authority_sha, _ = _authority_candidate(tmp_path)
    authority = open_rights_issue_authority(
        candidate_root=root,
        expected_candidate_manifest_sha256=manifest_sha,
        expected_authority_canonical_sha256=authority_sha,
    )
    index = pd.date_range("2024-01-02", periods=8, freq="B")
    bars = pd.DataFrame({"factor": [1.0] * 5 + [1.2] * 3}, index=index)
    assert index[5].date() == date(2024, 1, 9)

    class Candidate:
        def bars(self, symbol: str) -> pd.DataFrame:
            assert symbol == "000001.SZ"
            return bars

    application = rights_issue_application_audit(
        authority,
        symbols=("000001.SZ",),
        start=index[0].date(),
        end=index[-1].date(),
    )
    combined = combined_corporate_action_source_snapshot(
        dividend_snapshot_sha256=CorporateActionBook.empty().snapshot_sha256,
        authority=authority,
    )
    audit = audit_pattern_factor_action_coverage(
        Candidate(),
        symbols=("000001.SZ",),
        corporate_actions=CorporateActionBook.empty(),
        rights_issues=authority,
        start=index[0].date(),
        end=index[-1].date(),
        candidate_source_sha256="b" * 64,
        combined_corporate_action_snapshot_sha256=combined["snapshot_sha256"],
        rights_issue_application_sha256=application["application_sha256"],
    )

    assert audit["policy_sha256"] == FACTOR_ACTION_COVERAGE_POLICY_SHA256
    assert audit["material_factor_change_count"] == 1
    assert audit["bound_material_factor_change_count"] == 1
    assert audit["rights_issue_bound_material_factor_change_count"] == 1
    assert audit["unbound_material_factor_change_count"] == 0
    assert audit["bound_rights_issue_event_ids"] == (
        "RIGHTS_ISSUE:000001.SZ:2024-01-04",
    )
    assert audit["factor_account_participation_inference"] is False
    assert audit["coverage_complete"] is True

    policy_path = freeze_rights_issue_participation_policy(
        timing_root=tmp_path / "timing"
    )
    request = {
        "candidate_selection_authority": "EXPLICIT_PREPARE_ARGUMENT",
        "candidate_manifest": authority.candidate_manifest_reference,
        "candidate_manifest_sha256": authority.candidate_manifest_reference["sha256"],
        "candidate_dataset_manifest_sha256": authority.candidate_dataset_manifest_sha256,
        "candidate_revision": authority.candidate_revision,
        "rights_issue_authority": authority.authority_reference,
        "rights_issue_authority_canonical_sha256": authority.authority_canonical_sha256,
        "rights_issue_source_documents_sha256": authority.source_documents_sha256,
        "rights_issue_participation_policy": RIGHTS_ISSUE_PARTICIPATION_POLICY,
        "rights_issue_participation_policy_sha256": (
            RIGHTS_ISSUE_PARTICIPATION_POLICY_SHA256
        ),
        "rights_issue_participation_policy_artifact": {
            "sha256": _sha(policy_path.read_bytes())
        },
        "rights_issue_application_audit": application,
        "rights_issue_application_sha256": application["application_sha256"],
        "corporate_action_snapshot_sha256": CorporateActionBook.empty().snapshot_sha256,
        "corporate_action_source_snapshot": combined,
        "corporate_action_source_snapshot_sha256": combined["snapshot_sha256"],
        "factor_action_coverage_audit": audit,
        "snapshot_symbols": ["000001.SZ"],
        "population_spec": {
            "start": index[0].date().isoformat(),
            "end": index[-1].date().isoformat(),
        },
    }
    assert _rights_issue_request_contract_invalid(request) is False
    request["rights_issue_participation_policy_sha256"] = "0" * 64
    assert _rights_issue_request_contract_invalid(request) is True
