from __future__ import annotations

import json

import pytest

from backend.services.research_assistant.code_intelligence_core import (
    code_context_refs_for_pack,
    code_context_refs_for_worker,
    finalize_code_context_manifest,
)


def _safe_payload() -> dict[str, object]:
    return {
        "provider": "fixture",
        "query": "explain backend/services/research_assistant/service.py",
        "status": "ok",
        "as_of": "2026-06-02T10:00:00Z",
        "refs": [
            {
                "file_path": "backend/services/research_assistant/service.py",
                "symbol": "service",
                "edge_refs": [{"edge_id": "edge-service"}],
                "provenance": {"source": "fixture", "edge": "edge-service"},
                "summary": "summary only",
                "summary_ref": "tmp/summary.md",
                "detail_ref": "tmp/detail.md",
                "call_chain": [{"from": "service", "to": "agent"}],
                "impact_radius": {"affected_test_count": 1},
                "affected_tests": [{"test_path": "backend/tests/research_assistant/test_service.py", "classification": "recommended"}],
            }
        ],
    }


def test_pack_and_worker_refs_are_summary_ref_detail_only() -> None:
    manifest = finalize_code_context_manifest(_safe_payload())
    pack_refs = code_context_refs_for_pack(manifest)
    worker_refs = code_context_refs_for_worker(pack_refs)
    serialized = json.dumps({"pack": pack_refs, "worker": worker_refs}, ensure_ascii=False, sort_keys=True)

    assert "selected_nodes" not in serialized
    assert "selected_edges" not in serialized
    assert "source_text" not in serialized
    assert "summary_ref" in serialized and "detail_ref" in serialized
    assert len(serialized) < 6000


def test_large_or_raw_payload_keys_are_rejected() -> None:
    payload = _safe_payload()
    ref = dict(payload["refs"][0])  # type: ignore[index]
    ref["impact_radius"] = {"selected_nodes": [{"id": "node-1"}]}
    payload["refs"] = [ref]

    manifest = finalize_code_context_manifest(payload)

    assert manifest.status == "evidence_insufficient"
    assert manifest.refs == []
    assert "selected_nodes" in manifest.insufficient_refs[0]["error"]


def test_worker_ref_rejects_raw_payload_key() -> None:
    with pytest.raises(ValueError, match="token-unsafe"):
        code_context_refs_for_worker(
            [
                {
                    "file_path": "backend/a.py",
                    "symbol": "a",
                    "edge_refs": [{"edge_id": "edge"}],
                    "provenance": {"source": "fixture"},
                    "as_of": "2026-06-02T10:00:00Z",
                    "summary": "summary",
                    "detail_ref": "tmp/detail.md",
                    "source_text": "print('full body')",
                }
            ]
        )
