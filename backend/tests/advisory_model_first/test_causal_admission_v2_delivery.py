from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from scripts import advisory_causal_admission_v2_mve as cli
from backend.services.advisory_model_first.causal_admission_v2_contracts import (
    CAUSAL_ADMISSION_NEXT_TASK_BY_EVIDENCE,
    STATIC_ARM_ID,
)
from backend.services.advisory_model_first.causal_admission_v2_pipeline import (
    BUNDLE_MEMBERS,
    _find_existing_bundle,
    _publish_bundle,
    inspect_causal_admission_bundle,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256
from backend.tests.advisory_model_first.test_causal_admission_v2_pipeline import (
    build_test_request,
)


def _publish_fixture(tmp_path: Path):
    request = build_test_request(output_root=tmp_path.as_posix())
    day = pd.Timestamp("2025-01-02")
    frame = pd.DataFrame(
        {
            "phase": ["INNER"],
            "arm_id": [STATIC_ARM_ID],
            "decision_as_of_trade_date": [day],
            "instrument": ["000001.SZ"],
        }
    )
    summary = {
        "schema_version": "advisory_causal_admission_frontier_summary_v1",
        "request_sha256": request.request_sha256,
        "inner_selected_arm_id": STATIC_ARM_ID,
        "selected_arm_id": None,
        "evaluated_trial_count": 2,
        "selected_trial_count": 0,
        "evidence_class": "CAUSAL_ADMISSION_V2_1_SELECTED_ZERO",
        "next_task": CAUSAL_ADMISSION_NEXT_TASK_BY_EVIDENCE["CAUSAL_ADMISSION_V2_1_SELECTED_ZERO"],
    }
    summary["summary_sha256"] = canonical_json_sha256(summary)
    bundle = _publish_bundle(
        request=request,
        source_preflight={"schema_version": "test"},
        fit_receipts=(),
        predictions=frame,
        decisions=frame,
        daily=frame,
        episodes=frame,
        summary=summary,
        resource_report={"schema_version": "test", "peak_rss_bytes": 1},
    )
    return bundle, request


def test_bundle_is_content_addressed_and_registry_refs_use_final_path(tmp_path: Path) -> None:
    bundle, request = _publish_fixture(tmp_path)
    repeated, _ = _publish_fixture(tmp_path)
    inspected = inspect_causal_admission_bundle(bundle)
    records = json.loads((bundle / "registry_records.json").read_text(encoding="utf-8"))

    assert inspected["request_sha256"] == request.request_sha256
    assert repeated == bundle
    assert {item.name for item in bundle.iterdir()} == {*BUNDLE_MEMBERS, "manifest.json"}
    assert all(bundle.as_posix() in ref["artifact_uri"] for row in records for ref in row["evidence_refs"])
    assert len(records[0]["consumed_windows"]) == 2
    assert len(records[1]["consumed_windows"]) == 1
    assert json.loads((bundle / "resource_report.json").read_text())["temporary_bytes"] > 0


@pytest.mark.parametrize("mutation", ["member", "manifest", "extra"])
def test_bundle_tampering_fails_closed(tmp_path: Path, mutation: str) -> None:
    bundle, _ = _publish_fixture(tmp_path)
    if mutation == "member":
        (bundle / "frontier_summary.json").write_text("{}\n", encoding="utf-8")
    elif mutation == "manifest":
        manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
        manifest["frontier"]["selected_trial_count"] = 1
        (bundle / "manifest.json").write_text(json.dumps(manifest) + "\n", encoding="utf-8")
    else:
        (bundle / "unexpected.txt").write_text("x", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as caught:
        inspect_causal_admission_bundle(bundle)
    assert caught.value.reason_code == "ADVISORY_CAUSAL_BUNDLE_INVALID"


def test_existing_bundle_discovery_fails_closed_on_corrupt_request(tmp_path: Path) -> None:
    request = build_test_request(output_root=tmp_path.as_posix())
    corrupt = tmp_path / "causal_admission_bundles" / ("c" * 64)
    corrupt.mkdir(parents=True)
    (corrupt / "request.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(AdvisoryModelFirstError) as caught:
        _find_existing_bundle(request)

    assert caught.value.reason_code == "ADVISORY_CAUSAL_BUNDLE_INVALID"


def test_cli_missing_request_has_typed_failure(tmp_path: Path, capsys) -> None:
    exit_code = cli.main(["run", "--request", str(tmp_path / "missing.json")])
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["reason_code"] == "ADVISORY_CAUSAL_REQUEST_INVALID"
