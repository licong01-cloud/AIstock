from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = (
    ROOT
    / "scripts/qe_alpha_candidates/sector_rotation/ma_e19_semantic_equivalence_audit.py"
)
SPEC = importlib.util.spec_from_file_location("ma_e19_semantic_equivalence_audit", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


VINTAGE_WINDOWS = {
    "2024H2": {
        "train": {"start": "2018-08-01", "end": "2023-10-27"},
        "valid": {"start": "2023-11-28", "end": "2024-05-29"},
        "test": {"start": "2024-07-01", "end": "2024-12-31"},
    },
    "2025H1": {
        "train": {"start": "2018-08-01", "end": "2023-10-27"},
        "valid": {"start": "2023-11-28", "end": "2024-05-29"},
        "test": {"start": "2025-01-02", "end": "2025-06-30"},
    },
    "2025H2": {
        "train": {"start": "2018-08-01", "end": "2023-10-27"},
        "valid": {"start": "2023-11-28", "end": "2024-05-29"},
        "test": {"start": "2025-07-01", "end": "2025-12-31"},
    },
}


def _sha(value: str) -> str:
    return MODULE.canonical_sha256({"value": value})


def _component(*, arm_key: str, name: str, source_suffix: str = "baseline") -> dict[str, str]:
    return {
        "identity": f"{name}:{arm_key}:{source_suffix}",
        "source_sha256": _sha(f"source:{name}:{arm_key}:{source_suffix}"),
        "semantic_sha256": _sha(f"semantic:{name}:{arm_key}"),
    }


def _manifest(*, side: str, fixed_anchor: bool = False) -> dict[str, object]:
    arms: list[dict[str, object]] = []
    for vintage in ("2024H2", "2025H1", "2025H2"):
        for refit in ("fixed", "expanding", "rolling"):
            normalized_key = f"{vintage}:{refit}"
            raw_refit = "fixed_anchor" if fixed_anchor and normalized_key == "2024H2:fixed" else refit
            arms.append(
                {
                    "arm_id": f"{side}:{normalized_key}",
                    "vintage": vintage,
                    "refit": raw_refit,
                    "windows": json.loads(json.dumps(VINTAGE_WINDOWS[vintage])),
                    "components": {
                        name: _component(arm_key=normalized_key, name=name, source_suffix=side)
                        for name in MODULE.REQUIRED_COMPONENTS
                    },
                }
            )
    payload: dict[str, object] = {
        "schema_version": MODULE.MANIFEST_SCHEMA,
        "task_id": f"task-{side}",
        "arms": arms,
    }
    if side == "candidate":
        payload["release_evidence"] = {
            key: _sha(f"release:{key}") for key in MODULE.REQUIRED_RELEASE_EVIDENCE
        }
    payload["manifest_sha256"] = MODULE.canonical_sha256(payload)
    return payload


def _resign(manifest: dict[str, object]) -> None:
    manifest.pop("manifest_sha256", None)
    manifest["manifest_sha256"] = MODULE.canonical_sha256(manifest)


def _arm(manifest: dict[str, object], *, vintage: str, refit: str) -> dict[str, object]:
    for item in manifest["arms"]:  # type: ignore[index]
        if item["vintage"] == vintage and item["refit"] == refit:  # type: ignore[index]
            return item  # type: ignore[return-value]
    raise AssertionError((vintage, refit))


def _write(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def test_all_nine_arms_can_be_equivalent_despite_provenance_change() -> None:
    receipt = MODULE.audit_manifests(_manifest(side="baseline"), _manifest(side="candidate"))

    assert receipt["outcome"] == MODULE.OUTCOME_EQUIVALENT
    assert receipt["arm_count"] == 9
    assert receipt["equivalent_arm_count"] == 9
    assert receipt["rerun_arm_count"] == 0
    assert receipt["provenance_difference_count"] == 18 * 9
    assert all(row["outcome"] == MODULE.OUTCOME_EQUIVALENT for row in receipt["arms"])


def test_one_semantic_component_change_requires_full_rerun() -> None:
    candidate = _manifest(side="candidate")
    arm = _arm(candidate, vintage="2025H1", refit="rolling")
    arm["components"]["universe"]["semantic_sha256"] = _sha("changed")  # type: ignore[index]
    _resign(candidate)

    receipt = MODULE.audit_manifests(_manifest(side="baseline"), candidate)

    assert receipt["outcome"] == MODULE.OUTCOME_RERUN
    assert receipt["rerun_arm_count"] == 1
    failed = [row for row in receipt["arms"] if row["outcome"] == MODULE.OUTCOME_RERUN]
    assert [row["arm_key"] for row in failed] == ["2025H1:rolling"]
    assert failed[0]["mismatches"][0]["reason_code"] == "qe_ma_e19_component_semantics_changed"


def test_window_change_requires_full_rerun() -> None:
    candidate = _manifest(side="candidate")
    arm = _arm(candidate, vintage="2025H2", refit="fixed")
    arm["windows"]["test"]["start"] = "2025-07-02"  # type: ignore[index]
    _resign(candidate)

    receipt = MODULE.audit_manifests(_manifest(side="baseline"), candidate)

    assert receipt["outcome"] == MODULE.OUTCOME_RERUN
    failed = [row for row in receipt["arms"] if row["outcome"] == MODULE.OUTCOME_RERUN]
    assert failed[0]["mismatches"][0]["reason_code"] == "qe_ma_e19_window_semantics_changed"


def test_fixed_anchor_normalizes_only_for_2024h2() -> None:
    baseline = _manifest(side="baseline", fixed_anchor=True)

    receipt = MODULE.audit_manifests(baseline, _manifest(side="candidate"))

    assert receipt["outcome"] == MODULE.OUTCOME_EQUIVALENT


def test_missing_component_is_not_computable() -> None:
    candidate = _manifest(side="candidate")
    arm = _arm(candidate, vintage="2024H2", refit="fixed")
    arm["components"].pop("tradability")  # type: ignore[union-attr]
    _resign(candidate)

    with pytest.raises(MODULE.AuditInputError) as exc_info:
        MODULE.audit_manifests(_manifest(side="baseline"), candidate)

    assert exc_info.value.reason_code == "qe_ma_e19_component_set_invalid"


def test_missing_release_evidence_is_not_computable() -> None:
    candidate = _manifest(side="candidate")
    candidate.pop("release_evidence")
    _resign(candidate)

    with pytest.raises(MODULE.AuditInputError) as exc_info:
        MODULE.audit_manifests(_manifest(side="baseline"), candidate)

    assert exc_info.value.reason_code == "qe_ma_e19_release_evidence_missing"


def test_invalid_release_evidence_sha_is_not_computable() -> None:
    candidate = _manifest(side="candidate")
    candidate["release_evidence"]["candidate_signoff_sha256"] = "not-a-sha"  # type: ignore[index]
    _resign(candidate)

    with pytest.raises(MODULE.AuditInputError) as exc_info:
        MODULE.audit_manifests(_manifest(side="baseline"), candidate)

    assert exc_info.value.reason_code == "qe_ma_e19_release_evidence_invalid"


def test_manifest_hash_drift_is_not_computable() -> None:
    candidate = _manifest(side="candidate")
    candidate["task_id"] = "tampered-without-resigning"

    with pytest.raises(MODULE.AuditInputError) as exc_info:
        MODULE.audit_manifests(_manifest(side="baseline"), candidate)

    assert exc_info.value.reason_code == "qe_ma_e19_manifest_sha_mismatch"


def test_incomplete_arm_set_is_not_computable() -> None:
    candidate = _manifest(side="candidate")
    candidate["arms"].pop()  # type: ignore[union-attr]
    _resign(candidate)

    with pytest.raises(MODULE.AuditInputError) as exc_info:
        MODULE.audit_manifests(_manifest(side="baseline"), candidate)

    assert exc_info.value.reason_code == "qe_ma_e19_arm_set_invalid"


def test_duplicate_normalized_fixed_arm_is_not_computable() -> None:
    baseline = _manifest(side="baseline", fixed_anchor=True)
    duplicate = json.loads(json.dumps(_arm(baseline, vintage="2024H2", refit="fixed_anchor")))
    duplicate["refit"] = "fixed"
    duplicate["arm_id"] = "duplicate-fixed"
    baseline["arms"].append(duplicate)  # type: ignore[union-attr]
    _resign(baseline)

    with pytest.raises(MODULE.AuditInputError) as exc_info:
        MODULE.audit_manifests(baseline, _manifest(side="candidate"))

    assert exc_info.value.reason_code == "qe_ma_e19_arm_duplicate"


def test_invalid_component_source_sha_is_not_computable() -> None:
    baseline = _manifest(side="baseline")
    arm = _arm(baseline, vintage="2025H1", refit="expanding")
    arm["components"]["factor"]["source_sha256"] = "A" * 64  # type: ignore[index]
    _resign(baseline)

    with pytest.raises(MODULE.AuditInputError) as exc_info:
        MODULE.audit_manifests(baseline, _manifest(side="candidate"))

    assert exc_info.value.reason_code == "qe_ma_e19_component_source_sha_invalid"


def test_unknown_component_field_is_not_silently_ignored() -> None:
    candidate = _manifest(side="candidate")
    arm = _arm(candidate, vintage="2025H1", refit="fixed")
    arm["components"]["factor"]["unreviewed_semantics"] = "hidden"  # type: ignore[index]
    _resign(candidate)

    with pytest.raises(MODULE.AuditInputError) as exc_info:
        MODULE.audit_manifests(_manifest(side="baseline"), candidate)

    assert exc_info.value.reason_code == "qe_ma_e19_component_fields_invalid"


def test_unknown_manifest_field_requires_schema_revision() -> None:
    candidate = _manifest(side="candidate")
    candidate["unreviewed_contract"] = {"enabled": True}
    _resign(candidate)

    with pytest.raises(MODULE.AuditInputError) as exc_info:
        MODULE.audit_manifests(_manifest(side="baseline"), candidate)

    assert exc_info.value.reason_code == "qe_ma_e19_manifest_fields_invalid"


def test_unknown_release_evidence_field_is_not_silently_ignored() -> None:
    candidate = _manifest(side="candidate")
    candidate["release_evidence"]["worker_healthy_sha256"] = _sha("not-authoritative")  # type: ignore[index]
    _resign(candidate)

    with pytest.raises(MODULE.AuditInputError) as exc_info:
        MODULE.audit_manifests(_manifest(side="baseline"), candidate)

    assert exc_info.value.reason_code == "qe_ma_e19_release_evidence_invalid"


def test_receipt_is_byte_deterministic_for_same_inputs() -> None:
    baseline = _manifest(side="baseline")
    candidate = _manifest(side="candidate")

    first = MODULE.audit_manifests(baseline, candidate)
    second = MODULE.audit_manifests(baseline, candidate)

    assert MODULE._canonical_json_bytes(first) == MODULE._canonical_json_bytes(second)
    unsigned = dict(first)
    supplied = unsigned.pop("receipt_sha256")
    assert supplied == MODULE.canonical_sha256(unsigned)


def test_cli_writes_equivalent_receipt_atomically(tmp_path: Path) -> None:
    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"
    output_path = tmp_path / "receipt.json"
    _write(baseline_path, _manifest(side="baseline"))
    _write(candidate_path, _manifest(side="candidate"))

    exit_code = MODULE.main(
        [
            "--baseline-manifest",
            str(baseline_path),
            "--candidate-manifest",
            str(candidate_path),
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    receipt = json.loads(output_path.read_text(encoding="utf-8"))
    assert receipt["outcome"] == MODULE.OUTCOME_EQUIVALENT
    assert not list(tmp_path.glob(".*.tmp"))


def test_cli_writes_not_computable_receipt_and_exits_two(tmp_path: Path) -> None:
    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"
    output_path = tmp_path / "receipt.json"
    _write(baseline_path, _manifest(side="baseline"))
    candidate = _manifest(side="candidate")
    candidate.pop("release_evidence")
    _resign(candidate)
    _write(candidate_path, candidate)

    exit_code = MODULE.main(
        [
            "--baseline-manifest",
            str(baseline_path),
            "--candidate-manifest",
            str(candidate_path),
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 2
    receipt = json.loads(output_path.read_text(encoding="utf-8"))
    assert receipt["outcome"] == MODULE.OUTCOME_NOT_COMPUTABLE
    assert receipt["reason_codes"] == ["qe_ma_e19_release_evidence_missing"]


def test_cli_writes_rerun_receipt_and_exits_one(tmp_path: Path) -> None:
    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"
    output_path = tmp_path / "receipt.json"
    _write(baseline_path, _manifest(side="baseline"))
    candidate = _manifest(side="candidate")
    arm = _arm(candidate, vintage="2024H2", refit="rolling")
    arm["components"]["prediction"]["semantic_sha256"] = _sha("changed-prediction")  # type: ignore[index]
    _resign(candidate)
    _write(candidate_path, candidate)

    exit_code = MODULE.main(
        [
            "--baseline-manifest",
            str(baseline_path),
            "--candidate-manifest",
            str(candidate_path),
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 1
    receipt = json.loads(output_path.read_text(encoding="utf-8"))
    assert receipt["outcome"] == MODULE.OUTCOME_RERUN
    assert receipt["rerun_arm_count"] == 1


def test_cli_refuses_to_replace_an_input_manifest(tmp_path: Path) -> None:
    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"
    baseline = _manifest(side="baseline")
    _write(baseline_path, baseline)
    _write(candidate_path, _manifest(side="candidate"))

    exit_code = MODULE.main(
        [
            "--baseline-manifest",
            str(baseline_path),
            "--candidate-manifest",
            str(candidate_path),
            "--output",
            str(baseline_path),
        ]
    )

    assert exit_code == 2
    assert json.loads(baseline_path.read_text(encoding="utf-8")) == baseline


def test_tool_has_no_database_or_network_dependency() -> None:
    source = SCRIPT_PATH.read_text(encoding="utf-8")

    forbidden = (
        "psycopg",
        "sqlalchemy",
        "get_conn(",
        "requests.",
        "httpx.",
        "market.",
        "DATABASE_URL",
        "TDX_DB_",
    )
    assert all(token not in source for token in forbidden)


def test_tool_is_exactly_classified_as_non_runtime() -> None:
    from scripts.aistock_issue_workflow import _classify_runtime_impact

    result = _classify_runtime_impact(
        ["scripts/qe_alpha_candidates/sector_rotation/ma_e19_semantic_equivalence_audit.py"],
        root=ROOT,
    )

    assert result["runtime_impact"] == "none"
    assert result["observed_impacts"] == ["none"]
    assert result["runtime_files"] == []
    assert result["target_ids"] == []
