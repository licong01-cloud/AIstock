from __future__ import annotations

from backend.services.research_assistant.code_intelligence_core import (
    code_context_manifest_bytes,
    finalize_code_context_manifest,
)


def _manifest_payload() -> dict[str, object]:
    return {
        "provider": "fixture",
        "query": "explain module impact",
        "status": "ok",
        "as_of": "2026-06-02T10:00:00Z",
        "refs": [
            {
                "file_path": r"backend\\services\\research_assistant\\service.py",
                "symbol": "service",
                "edge_refs": [{"edge_id": "edge-b"}],
                "provenance": {"source": "fixture", "file": "backend/services/research_assistant/service.py"},
                "summary": "service summary",
                "summary_ref": "tmp/summary-b.md",
                "detail_ref": "tmp/detail-b.md",
                "call_chain": [{"from": "service", "to": "runtime"}],
                "impact_radius": {"affected_test_count": 1},
                "affected_tests": [{"test_path": r"backend\\tests\\research_assistant\\test_service.py", "classification": "recommended"}],
            },
            {
                "file_path": "backend/services/research_assistant/code_intelligence_core.py",
                "symbol": "code_intelligence_core",
                "edge_refs": [{"edge_id": "edge-a"}],
                "provenance": {"source": "fixture", "file": "backend/services/research_assistant/code_intelligence_core.py"},
                "summary": "core summary",
                "summary_ref": "tmp/summary-a.md",
                "detail_ref": "tmp/detail-a.md",
                "call_chain": [{"from": "core", "to": "worker"}],
                "impact_radius": {"affected_test_count": 1},
                "affected_tests": [{"test_path": "backend/tests/research_assistant/test_code_intel_determinism.py", "classification": "impacted"}],
            },
        ],
    }


def test_code_context_manifest_is_byte_identical_for_same_input() -> None:
    first = finalize_code_context_manifest(_manifest_payload())
    second = finalize_code_context_manifest(_manifest_payload())

    assert code_context_manifest_bytes(first) == code_context_manifest_bytes(second)
    assert first.refs[0].file_path == "backend/services/research_assistant/code_intelligence_core.py"
    assert first.refs[1].file_path == "backend/services/research_assistant/service.py"
    assert first.refs[1].affected_tests[0].test_path == "backend/tests/research_assistant/test_service.py"


def test_code_context_manifest_dedupes_and_stably_sorts_refs() -> None:
    payload = _manifest_payload()
    payload["refs"] = list(reversed(payload["refs"])) + [dict(payload["refs"][0])]  # type: ignore[index]
    manifest = finalize_code_context_manifest(payload)

    assert [(ref.file_path, ref.symbol, ref.edge_refs[0]["edge_id"]) for ref in manifest.refs] == [
        ("backend/services/research_assistant/code_intelligence_core.py", "code_intelligence_core", "edge-a"),
        ("backend/services/research_assistant/service.py", "service", "edge-b"),
    ]
