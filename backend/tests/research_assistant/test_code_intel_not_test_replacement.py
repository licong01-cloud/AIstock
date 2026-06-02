from __future__ import annotations

import json

from backend.services.research_assistant.code_intelligence_core import (
    code_context_refs_for_pack,
    code_context_refs_for_worker,
    finalize_code_context_manifest,
)


def _payload_with_test_classification(classification: str) -> dict[str, object]:
    return {
        "provider": "fixture",
        "query": "explain backend/services/research_assistant/service.py",
        "status": "ok",
        "refs": [
            {
                "file_path": "backend/services/research_assistant/service.py",
                "symbol": "service",
                "edge_refs": [{"edge_id": "edge"}],
                "provenance": {"source": "fixture"},
                "as_of": "2026-06-02T10:00:00Z",
                "summary": "summary",
                "summary_ref": "tmp/summary.md",
                "detail_ref": "tmp/detail.md",
                "affected_tests": [
                    {
                        "test_path": "backend/tests/research_assistant/test_service.py",
                        "classification": classification,
                        "source_ref": "tmp/affected-tests.json",
                    }
                ],
            }
        ],
    }


def test_affected_tests_are_impacted_or_recommended_only() -> None:
    manifest = finalize_code_context_manifest(_payload_with_test_classification("impacted"))
    refs = code_context_refs_for_pack(manifest)
    worker_refs = code_context_refs_for_worker(refs)
    serialized = json.dumps({"pack": refs, "worker": worker_refs}, sort_keys=True)

    assert manifest.status == "ok"
    assert "impacted" in serialized
    assert "passed" not in serialized
    assert "verified" not in serialized
    assert "ci_passed" not in serialized


def test_passed_or_verified_test_claims_are_rejected() -> None:
    passed = finalize_code_context_manifest(_payload_with_test_classification("passed"))
    verified_payload = _payload_with_test_classification("recommended")
    verified_payload["refs"][0]["affected_tests"][0]["status"] = "verified"  # type: ignore[index]
    verified = finalize_code_context_manifest(verified_payload)

    assert passed.status == "evidence_insufficient"
    assert passed.refs == []
    assert verified.status == "evidence_insufficient"
    assert verified.refs == []
