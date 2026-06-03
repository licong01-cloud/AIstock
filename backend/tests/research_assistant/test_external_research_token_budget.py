from __future__ import annotations

import pytest

from backend.services.research_assistant.external_research import (
    ExternalEvidenceItem,
    assert_token_safe,
    evidence_summary_response,
)


def test_external_research_search_payload_does_not_inline_heavy_fields() -> None:
    item = ExternalEvidenceItem(
        title="long report",
        summary="long summary " * 500,
        url="https://example.org/long-report",
        source="web_index",
        as_of="2026-06-01",
        evidence_ref="external-evidence:long",
        provider="fake",
    )
    payload = evidence_summary_response(domain="external_research.web", items=[item], limit=1)
    rendered = str(payload)

    assert len(payload["items"][0]["summary"]) <= 600
    for forbidden in ("raw_html", "raw_pdf", "full_text", "provider_raw_response"):
        assert f"'{forbidden}':" not in rendered
        assert f'"{forbidden}":' not in rendered
    assert payload["summary_first"] is True


def test_external_research_payload_guard_fails_on_full_text_or_large_values() -> None:
    with pytest.raises(ValueError, match="full_text"):
        assert_token_safe({"full_text": "not allowed"})

    with pytest.raises(ValueError, match="exceeds summary budget"):
        assert_token_safe({"summary": "x" * 6000})
