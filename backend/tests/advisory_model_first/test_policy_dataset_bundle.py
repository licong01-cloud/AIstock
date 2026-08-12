from __future__ import annotations

import json

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.policy_dataset_bundle import (
    load_policy_dataset_bundle,
    publish_policy_dataset_bundle,
)
from backend.tests.advisory_model_first.test_policy_contracts import _request


def test_policy_dataset_bundle_is_content_addressed_and_readback_detects_tampering(tmp_path) -> None:
    request = _request(output_root=str(tmp_path))
    frame = pd.DataFrame({"decision_as_of_trade_date": [pd.Timestamp("2026-01-02")], "value": [1.0]})
    bundle_id, path, first = publish_policy_dataset_bundle(
        request=request,
        rankings=frame,
        labels=frame,
        label_coverage=[{"status": "AVAILABLE"}],
        shadow_daily=frame,
        shadow_episodes=frame,
        shadow_metrics={"day_count": 1},
        cpcv_payload={"paths": []},
        pbo_receipt={"status": "NOT_COMPUTABLE"},
        source_schema_receipt={"schema": "ok"},
        resource_report={"peak_rss_bytes": 1},
    )
    second_id, second_path, second = publish_policy_dataset_bundle(
        request=request,
        rankings=frame,
        labels=frame,
        label_coverage=[{"status": "AVAILABLE"}],
        shadow_daily=frame,
        shadow_episodes=frame,
        shadow_metrics={"day_count": 1},
        cpcv_payload={"paths": []},
        pbo_receipt={"status": "NOT_COMPUTABLE"},
        source_schema_receipt={"schema": "ok"},
        resource_report={"peak_rss_bytes": 999},
    )
    assert second_id == bundle_id
    assert second_path == path
    assert second == first
    target = path / "candidate_label_coverage.json"
    target.write_text(json.dumps([{"status": "tampered"}]), encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError, match="differs from its manifest"):
        load_policy_dataset_bundle(path, expected_bundle_id=bundle_id)
