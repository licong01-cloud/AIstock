from __future__ import annotations

import pytest

from backend.services.mcp_payload_budget import (
    artifact_ref,
    assert_summary_payload,
    clamp_limit,
    clamp_offset,
    strip_forbidden_fields,
    summary_envelope,
)


def test_clamp_limit_and_offset_contract() -> None:
    assert clamp_limit(None) == 20
    assert clamp_limit(0) == 20
    assert clamp_limit("bad") == 20
    assert clamp_limit(500) == 100
    assert clamp_limit(50) == 50
    assert clamp_offset(None) == 0
    assert clamp_offset(-10) == 0
    assert clamp_offset(3) == 3


def test_forbidden_fields_are_stripped_recursively() -> None:
    payload = {
        "factor_name": "alpha",
        "metrics_json": {"huge": True},
        "nested": {"config_json": {"raw": True}, "safe": 1},
        "rows": [1, 2, 3],
        "children": [{"model_weights": "...", "ok": True}],
    }
    stripped = strip_forbidden_fields(payload)
    assert "metrics_json" not in stripped
    assert "rows" not in stripped
    assert "config_json" not in stripped["nested"]
    assert "model_weights" not in stripped["children"][0]
    assert stripped["children"][0]["ok"] is True


def test_summary_envelope_omits_heavy_payload_and_uses_refs() -> None:
    payload = summary_envelope(
        domain="factor_correlation",
        items=[{"factor": "a", "correlation_matrix": [[1.0]], "score": 1}],
        limit=500,
        offset=0,
        artifact_refs=[artifact_ref("matrix", "artifact://matrix/1", {"rows": [1, 2, 3], "shape": [100, 100]})],
        omitted_sections=["correlation_matrix"],
    )
    assert payload["pagination"]["limit"] == 100
    assert payload["items"] == [{"factor": "a", "score": 1}]
    assert payload["artifact_refs"][0]["inline"] is False
    assert "rows" not in payload["artifact_refs"][0].get("metadata", {})
    assert_summary_payload(payload)


def test_assert_summary_payload_rejects_heavy_fields() -> None:
    with pytest.raises(ValueError, match="metrics_json"):
        assert_summary_payload({"items": [{"metrics_json": {}}]})
