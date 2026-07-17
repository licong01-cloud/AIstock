from __future__ import annotations

import json

import pytest

from backend.services.hmm_evolution.asset_content_policy import sanitize_asset_text
from backend.services.hmm_evolution.errors import QEAssetContentInvalidError


def test_json_asset_policy_recursively_redacts_secrets_and_paths() -> None:
    result = sanitize_asset_text(
        json.dumps(
            {
                "token": "abc",
                "nested": {
                    "workspace": "F:/Dev/AIstock/private",
                    "authorization": "Bearer abc.def",
                },
            }
        ).encode(),
        relative_path="reports/config.json",
        content_type="application/json",
    )

    assert result.schema_kind == "json"
    assert result.redaction_count >= 3
    assert "abc.def" not in result.text
    assert "F:/Dev/AIstock" not in result.text


def test_json_asset_policy_rejects_invalid_json_instead_of_showing_raw_text() -> None:
    with pytest.raises(QEAssetContentInvalidError, match="invalid"):
        sanitize_asset_text(
            b"{not-json}",
            relative_path="reports/config.json",
            content_type="application/json",
        )
