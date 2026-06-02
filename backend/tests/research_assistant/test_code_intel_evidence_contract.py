from __future__ import annotations

from pathlib import Path

from backend.services.research_assistant.code_intelligence_core import (
    build_code_context_manifest,
    finalize_code_context_manifest,
)


class RawProvider:
    def __init__(self, payload):
        self.payload = payload

    def query_code_context(self, _request):
        return self.payload


def _raw_ref() -> dict[str, object]:
    return {
        "file_path": "backend/services/research_assistant/service.py",
        "symbol": "build_context_pack",
        "edge_refs": [{"edge_id": "edge"}],
        "provenance": {"source": "fixture"},
        "as_of": "2026-06-02T10:00:00Z",
        "summary": "summary",
        "summary_ref": "tmp/summary.md",
        "detail_ref": "tmp/detail.md",
        "affected_tests": [{"test_path": "backend/tests/research_assistant/test_service.py", "classification": "recommended"}],
    }


def test_missing_provenance_or_as_of_does_not_enter_final_pack() -> None:
    missing_provenance = _raw_ref()
    missing_provenance.pop("provenance")
    missing_as_of = _raw_ref()
    missing_as_of.pop("as_of")
    payload = {
        "provider": "fixture",
        "query": "explain backend/services/research_assistant/service.py",
        "status": "ok",
        "refs": [missing_provenance, missing_as_of],
    }

    manifest = build_code_context_manifest(
        provider=RawProvider(payload),
        query="explain backend/services/research_assistant/service.py",
        changed_files=["backend/services/research_assistant/service.py"],
    )

    assert manifest.status == "evidence_insufficient"
    assert manifest.refs == []
    assert len(manifest.insufficient_refs) == 2
    assert manifest.reason_code == "evidence_insufficient"


def test_valid_code_conclusion_requires_file_symbol_edge_provenance_and_as_of() -> None:
    manifest = finalize_code_context_manifest({"provider": "fixture", "status": "ok", "refs": [_raw_ref()]})

    ref = manifest.refs[0]
    assert ref.file_path
    assert ref.symbol
    assert ref.edge_refs[0]["edge_id"] == "edge"
    assert ref.provenance["source"] == "fixture"
    assert ref.as_of == "2026-06-02T10:00:00Z"


def test_phase8_code_does_not_fabricate_as_of_with_current_clock() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    phase8_files = [
        repo_root / "backend/services/research_assistant/code_intelligence_core.py",
        repo_root / "backend/services/research_assistant/code_intelligence_adapter_provider.py",
        repo_root / "backend/services/research_assistant/code_context_refs_repository.py",
    ]
    for path in phase8_files:
        text = path.read_text(encoding="utf-8")
        assert "datetime.now" not in text
        assert "date.today" not in text
