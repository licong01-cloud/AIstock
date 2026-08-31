from __future__ import annotations

from backend.services.advisory_model_first import feature_schema_v1, feature_schema_v2
from backend.services.advisory_model_first.suspension_aware_bar_policy import (
    BAR_POLICY_PAYLOAD,
)


def _identities() -> tuple[dict[str, object], dict[str, object]]:
    return (
        {"calendar_sha256": "a" * 64, "cutoff": "2026-06-30"},
        {"file_sha256": "b" * 64, "cutoff": "2026-06-30"},
    )


def test_schema_v2_is_identity_bound_and_keeps_v1_immutable() -> None:
    calendar, suspend = _identities()
    payload = feature_schema_v2.build_feature_schema_payload(
        market_calendar_identity=calendar, suspend_sidecar_identity=suspend
    )
    assert payload["schema_version"] == "advisory_feature_schema_v2_suspension_aware"
    assert payload["bar_policy"] == BAR_POLICY_PAYLOAD
    assert payload["market_calendar_identity"] == calendar
    assert payload["suspend_sidecar_identity"] == suspend
    assert feature_schema_v1.FEATURE_SCHEMA_PAYLOAD["schema_version"] == "advisory_feature_schema_v1"


def test_schema_v2_hash_changes_with_bound_data_identity() -> None:
    calendar, suspend = _identities()
    first = feature_schema_v2.feature_schema_hash(market_calendar_identity=calendar, suspend_sidecar_identity=suspend)
    second = feature_schema_v2.feature_schema_hash(
        market_calendar_identity={**calendar, "cutoff": "2026-06-29"},
        suspend_sidecar_identity=suspend,
    )
    assert len(first) == 64
    assert first != second


def test_suspension_conditional_features_are_optional_with_indicators() -> None:
    for column in feature_schema_v2.SUSPENSION_CONDITIONAL_OPTIONAL_COLUMNS:
        assert column in feature_schema_v2.OPTIONAL_FEATURE_COLUMNS
        assert f"{column}__missing" in feature_schema_v2.MISSING_INDICATOR_COLUMNS
    for column in feature_schema_v2.SUSPENSION_FEATURE_COLUMNS:
        assert column in feature_schema_v2.MODEL_FEATURE_COLUMNS
