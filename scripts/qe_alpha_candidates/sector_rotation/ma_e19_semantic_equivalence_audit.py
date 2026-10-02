#!/usr/bin/env python
"""Fail-closed semantic-equivalence audit for the completed MA-E19R2 arms.

The tool compares two frozen, self-hashed arm-set manifests.  It never reads a
database, calls an API, submits QE work, or mutates a dataset.  Provenance may
change between immutable releases, but every arm's window-scoped semantics
must remain byte-identical before an old arm can be reused.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence


MANIFEST_SCHEMA = "qe_ma_e19_arm_set_manifest_v1"
RECEIPT_SCHEMA = "qe_ma_e19_semantic_equivalence_receipt_v1"
OUTCOME_EQUIVALENT = "SEMANTIC_EQUIVALENT"
OUTCOME_RERUN = "RERUN_REQUIRED"
OUTCOME_NOT_COMPUTABLE = "NOT_COMPUTABLE"

REQUIRED_COMPONENTS = (
    "dataset",
    "calendar",
    "universe",
    "tradability",
    "factor",
    "label",
    "prediction",
    "order",
    "strategy",
)
REQUIRED_WINDOWS = ("train", "valid", "test")
EXPECTED_ARM_KEYS = (
    "2024H2:fixed",
    "2024H2:expanding",
    "2024H2:rolling",
    "2025H1:fixed",
    "2025H1:expanding",
    "2025H1:rolling",
    "2025H2:fixed",
    "2025H2:expanding",
    "2025H2:rolling",
)
REQUIRED_RELEASE_EVIDENCE = (
    "candidate_signoff_sha256",
    "catalog_readback_sha256",
    "node_distribution_sha256",
    "active_activation_sha256",
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class AuditInputError(ValueError):
    reason_code: str
    detail: str

    def __str__(self) -> str:
        return f"{self.reason_code}: {self.detail}"


@dataclass(frozen=True)
class ValidatedManifest:
    task_id: str
    manifest_sha256: str
    arms: Mapping[str, Mapping[str, Any]]
    raw: Mapping[str, Any]


def _canonical_json_bytes(payload: Any) -> bytes:
    try:
        text = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise AuditInputError("qe_ma_e19_json_not_canonical", str(exc)) from exc
    return text.encode("utf-8")


def canonical_sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _require_mapping(value: Any, *, reason_code: str, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AuditInputError(reason_code, f"{label} must be a JSON object")
    return value


def _require_text(value: Any, *, reason_code: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AuditInputError(reason_code, f"{label} must be a non-empty string")
    return value.strip()


def _require_sha256(value: Any, *, reason_code: str, label: str) -> str:
    text = _require_text(value, reason_code=reason_code, label=label)
    if not SHA256_RE.fullmatch(text):
        raise AuditInputError(reason_code, f"{label} must be a lowercase 64-character SHA256")
    return text


def _parse_date(value: Any, *, label: str) -> date:
    text = _require_text(value, reason_code="qe_ma_e19_window_invalid", label=label)
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise AuditInputError("qe_ma_e19_window_invalid", f"{label} must be YYYY-MM-DD") from exc


def _normalize_refit(vintage: str, refit: str) -> str:
    if refit == "fixed_anchor":
        if vintage != "2024H2":
            raise AuditInputError(
                "qe_ma_e19_arm_key_invalid",
                "fixed_anchor is only valid for the 2024H2 arm",
            )
        return "fixed"
    if refit not in {"fixed", "expanding", "rolling"}:
        raise AuditInputError("qe_ma_e19_arm_key_invalid", f"unsupported refit={refit!r}")
    return refit


def _validate_windows(value: Any, *, arm_key: str) -> Mapping[str, Mapping[str, str]]:
    windows = _require_mapping(
        value,
        reason_code="qe_ma_e19_window_invalid",
        label=f"arms[{arm_key}].windows",
    )
    if set(windows) != set(REQUIRED_WINDOWS):
        raise AuditInputError(
            "qe_ma_e19_window_invalid",
            f"arms[{arm_key}].windows must contain exactly {list(REQUIRED_WINDOWS)}",
        )
    normalized: dict[str, Mapping[str, str]] = {}
    parsed: dict[str, tuple[date, date]] = {}
    for window_name in REQUIRED_WINDOWS:
        window = _require_mapping(
            windows[window_name],
            reason_code="qe_ma_e19_window_invalid",
            label=f"arms[{arm_key}].windows.{window_name}",
        )
        if set(window) != {"start", "end"}:
            raise AuditInputError(
                "qe_ma_e19_window_invalid",
                f"arms[{arm_key}].windows.{window_name} must contain exactly start/end",
            )
        start = _parse_date(window["start"], label=f"arms[{arm_key}].windows.{window_name}.start")
        end = _parse_date(window["end"], label=f"arms[{arm_key}].windows.{window_name}.end")
        if start > end:
            raise AuditInputError(
                "qe_ma_e19_window_invalid",
                f"arms[{arm_key}].windows.{window_name} starts after it ends",
            )
        parsed[window_name] = (start, end)
        normalized[window_name] = {"start": start.isoformat(), "end": end.isoformat()}
    if not (parsed["train"][1] < parsed["valid"][0] and parsed["valid"][1] < parsed["test"][0]):
        raise AuditInputError(
            "qe_ma_e19_window_overlap",
            f"arms[{arm_key}] requires train < valid < test without overlap",
        )
    return normalized


def _validate_components(value: Any, *, arm_key: str) -> Mapping[str, Mapping[str, str]]:
    components = _require_mapping(
        value,
        reason_code="qe_ma_e19_component_missing",
        label=f"arms[{arm_key}].components",
    )
    if set(components) != set(REQUIRED_COMPONENTS):
        missing = sorted(set(REQUIRED_COMPONENTS) - set(components))
        extra = sorted(set(components) - set(REQUIRED_COMPONENTS))
        raise AuditInputError(
            "qe_ma_e19_component_set_invalid",
            f"arms[{arm_key}] missing={missing} extra={extra}",
        )
    normalized: dict[str, Mapping[str, str]] = {}
    for name in REQUIRED_COMPONENTS:
        component = _require_mapping(
            components[name],
            reason_code="qe_ma_e19_component_invalid",
            label=f"arms[{arm_key}].components.{name}",
        )
        expected_fields = {"identity", "source_sha256", "semantic_sha256"}
        if set(component) != expected_fields:
            raise AuditInputError(
                "qe_ma_e19_component_fields_invalid",
                f"arms[{arm_key}].components.{name} must contain exactly {sorted(expected_fields)}",
            )
        normalized[name] = {
            "identity": _require_text(
                component.get("identity"),
                reason_code="qe_ma_e19_component_identity_missing",
                label=f"arms[{arm_key}].components.{name}.identity",
            ),
            "source_sha256": _require_sha256(
                component.get("source_sha256"),
                reason_code="qe_ma_e19_component_source_sha_invalid",
                label=f"arms[{arm_key}].components.{name}.source_sha256",
            ),
            "semantic_sha256": _require_sha256(
                component.get("semantic_sha256"),
                reason_code="qe_ma_e19_component_semantic_sha_invalid",
                label=f"arms[{arm_key}].components.{name}.semantic_sha256",
            ),
        }
    return normalized


def _validate_release_evidence(payload: Mapping[str, Any]) -> None:
    evidence = _require_mapping(
        payload.get("release_evidence"),
        reason_code="qe_ma_e19_release_evidence_missing",
        label="candidate.release_evidence",
    )
    if set(evidence) != set(REQUIRED_RELEASE_EVIDENCE):
        raise AuditInputError(
            "qe_ma_e19_release_evidence_invalid",
            f"candidate.release_evidence must contain exactly {list(REQUIRED_RELEASE_EVIDENCE)}",
        )
    for key in REQUIRED_RELEASE_EVIDENCE:
        _require_sha256(
            evidence.get(key),
            reason_code="qe_ma_e19_release_evidence_invalid",
            label=f"candidate.release_evidence.{key}",
        )


def validate_manifest(payload: Any, *, side: str) -> ValidatedManifest:
    manifest = _require_mapping(
        payload,
        reason_code="qe_ma_e19_manifest_invalid",
        label=f"{side} manifest",
    )
    if side == "candidate" and "release_evidence" not in manifest:
        raise AuditInputError(
            "qe_ma_e19_release_evidence_missing",
            "candidate.release_evidence must be present",
        )
    expected_manifest_fields = {"schema_version", "task_id", "arms", "manifest_sha256"}
    if side == "candidate":
        expected_manifest_fields.add("release_evidence")
    if set(manifest) != expected_manifest_fields:
        raise AuditInputError(
            "qe_ma_e19_manifest_fields_invalid",
            f"{side} manifest must contain exactly {sorted(expected_manifest_fields)}",
        )
    if manifest.get("schema_version") != MANIFEST_SCHEMA:
        raise AuditInputError(
            "qe_ma_e19_manifest_schema_invalid",
            f"{side}.schema_version must be {MANIFEST_SCHEMA}",
        )
    supplied_sha = _require_sha256(
        manifest.get("manifest_sha256"),
        reason_code="qe_ma_e19_manifest_sha_invalid",
        label=f"{side}.manifest_sha256",
    )
    unsigned = dict(manifest)
    unsigned.pop("manifest_sha256", None)
    calculated_sha = canonical_sha256(unsigned)
    if supplied_sha != calculated_sha:
        raise AuditInputError(
            "qe_ma_e19_manifest_sha_mismatch",
            f"{side}.manifest_sha256 supplied={supplied_sha} calculated={calculated_sha}",
        )
    task_id = _require_text(
        manifest.get("task_id"),
        reason_code="qe_ma_e19_task_identity_missing",
        label=f"{side}.task_id",
    )
    raw_arms = manifest.get("arms")
    if not isinstance(raw_arms, Sequence) or isinstance(raw_arms, (str, bytes)):
        raise AuditInputError("qe_ma_e19_arm_set_invalid", f"{side}.arms must be a JSON array")
    normalized_arms: dict[str, Mapping[str, Any]] = {}
    for index, raw_arm in enumerate(raw_arms):
        arm = _require_mapping(
            raw_arm,
            reason_code="qe_ma_e19_arm_invalid",
            label=f"{side}.arms[{index}]",
        )
        expected_arm_fields = {"arm_id", "vintage", "refit", "windows", "components"}
        if set(arm) != expected_arm_fields:
            raise AuditInputError(
                "qe_ma_e19_arm_fields_invalid",
                f"{side}.arms[{index}] must contain exactly {sorted(expected_arm_fields)}",
            )
        vintage = _require_text(
            arm.get("vintage"),
            reason_code="qe_ma_e19_arm_key_invalid",
            label=f"{side}.arms[{index}].vintage",
        )
        refit_raw = _require_text(
            arm.get("refit"),
            reason_code="qe_ma_e19_arm_key_invalid",
            label=f"{side}.arms[{index}].refit",
        )
        refit = _normalize_refit(vintage, refit_raw)
        arm_key = f"{vintage}:{refit}"
        if arm_key in normalized_arms:
            raise AuditInputError("qe_ma_e19_arm_duplicate", f"{side} contains duplicate arm {arm_key}")
        normalized_arms[arm_key] = {
            "arm_id": _require_text(
                arm.get("arm_id"),
                reason_code="qe_ma_e19_arm_identity_missing",
                label=f"{side}.arms[{index}].arm_id",
            ),
            "vintage": vintage,
            "refit": refit,
            "windows": _validate_windows(arm.get("windows"), arm_key=arm_key),
            "components": _validate_components(arm.get("components"), arm_key=arm_key),
        }
    if tuple(sorted(normalized_arms)) != tuple(sorted(EXPECTED_ARM_KEYS)):
        missing = sorted(set(EXPECTED_ARM_KEYS) - set(normalized_arms))
        extra = sorted(set(normalized_arms) - set(EXPECTED_ARM_KEYS))
        raise AuditInputError(
            "qe_ma_e19_arm_set_invalid",
            f"{side} must contain the exact nine-arm set; missing={missing} extra={extra}",
        )
    if side == "candidate":
        _validate_release_evidence(manifest)
    return ValidatedManifest(
        task_id=task_id,
        manifest_sha256=supplied_sha,
        arms=normalized_arms,
        raw=manifest,
    )


def audit_manifests(baseline_payload: Any, candidate_payload: Any) -> Mapping[str, Any]:
    baseline = validate_manifest(baseline_payload, side="baseline")
    candidate = validate_manifest(candidate_payload, side="candidate")
    arm_receipts: list[Mapping[str, Any]] = []
    rerun_count = 0
    provenance_difference_count = 0
    for arm_key in EXPECTED_ARM_KEYS:
        baseline_arm = baseline.arms[arm_key]
        candidate_arm = candidate.arms[arm_key]
        mismatches: list[Mapping[str, str]] = []
        provenance: list[Mapping[str, str]] = []
        for window_name in REQUIRED_WINDOWS:
            if baseline_arm["windows"][window_name] != candidate_arm["windows"][window_name]:
                mismatches.append(
                    {
                        "path": f"windows.{window_name}",
                        "baseline": canonical_sha256(baseline_arm["windows"][window_name]),
                        "candidate": canonical_sha256(candidate_arm["windows"][window_name]),
                        "reason_code": "qe_ma_e19_window_semantics_changed",
                    }
                )
        for component_name in REQUIRED_COMPONENTS:
            baseline_component = baseline_arm["components"][component_name]
            candidate_component = candidate_arm["components"][component_name]
            if baseline_component["semantic_sha256"] != candidate_component["semantic_sha256"]:
                mismatches.append(
                    {
                        "path": f"components.{component_name}.semantic_sha256",
                        "baseline": baseline_component["semantic_sha256"],
                        "candidate": candidate_component["semantic_sha256"],
                        "reason_code": "qe_ma_e19_component_semantics_changed",
                    }
                )
            for field in ("identity", "source_sha256"):
                if baseline_component[field] != candidate_component[field]:
                    provenance.append(
                        {
                            "path": f"components.{component_name}.{field}",
                            "baseline": baseline_component[field],
                            "candidate": candidate_component[field],
                            "reason_code": "qe_ma_e19_component_provenance_changed",
                        }
                    )
        outcome = OUTCOME_RERUN if mismatches else OUTCOME_EQUIVALENT
        if mismatches:
            rerun_count += 1
        provenance_difference_count += len(provenance)
        arm_receipts.append(
            {
                "arm_key": arm_key,
                "baseline_arm_id": baseline_arm["arm_id"],
                "candidate_arm_id": candidate_arm["arm_id"],
                "outcome": outcome,
                "mismatches": mismatches,
                "provenance_differences": provenance,
            }
        )
    outcome = OUTCOME_RERUN if rerun_count else OUTCOME_EQUIVALENT
    receipt: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA,
        "outcome": outcome,
        "reason_codes": (
            ["qe_ma_e19_semantic_mismatch_requires_full_rerun"]
            if rerun_count
            else ["qe_ma_e19_all_nine_arms_semantically_equivalent"]
        ),
        "baseline_task_id": baseline.task_id,
        "candidate_task_id": candidate.task_id,
        "baseline_manifest_sha256": baseline.manifest_sha256,
        "candidate_manifest_sha256": candidate.manifest_sha256,
        "arm_count": len(arm_receipts),
        "equivalent_arm_count": len(arm_receipts) - rerun_count,
        "rerun_arm_count": rerun_count,
        "provenance_difference_count": provenance_difference_count,
        "arms": arm_receipts,
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    return receipt


def _load_json_file(path: Path, *, side: str) -> Mapping[str, Any]:
    if path.is_symlink():
        raise AuditInputError("qe_ma_e19_manifest_symlink_forbidden", f"{side} path is a symlink: {path}")
    if not path.is_file():
        raise AuditInputError("qe_ma_e19_manifest_file_missing", f"{side} file does not exist: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AuditInputError("qe_ma_e19_manifest_file_invalid", f"{side}: {exc}") from exc
    return _require_mapping(
        value,
        reason_code="qe_ma_e19_manifest_invalid",
        label=f"{side} manifest",
    )


def _not_computable_receipt(error: AuditInputError) -> Mapping[str, Any]:
    receipt: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA,
        "outcome": OUTCOME_NOT_COMPUTABLE,
        "reason_codes": [error.reason_code],
        "detail": error.detail,
        "arm_count": 0,
        "equivalent_arm_count": 0,
        "rerun_arm_count": 0,
        "provenance_difference_count": 0,
        "arms": [],
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    return receipt


def _write_receipt(path: Path, receipt: Mapping[str, Any], *, protected_inputs: Sequence[Path]) -> None:
    resolved_output = path.resolve(strict=False)
    if any(resolved_output == item.resolve(strict=False) for item in protected_inputs):
        raise AuditInputError("qe_ma_e19_output_overlaps_input", "output path must not replace an input manifest")
    if path.exists() and path.is_symlink():
        raise AuditInputError("qe_ma_e19_output_symlink_forbidden", f"output path is a symlink: {path}")
    parent = path.parent
    if not parent.is_dir() or parent.is_symlink():
        raise AuditInputError(
            "qe_ma_e19_output_parent_invalid",
            f"output parent must be an existing non-symlink directory: {parent}",
        )
    encoded = _canonical_json_bytes(receipt) + b"\n"
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit semantic equivalence of the nine completed MA-E19R2 arms.",
    )
    parser.add_argument("--baseline-manifest", type=Path, required=True)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    inputs = (args.baseline_manifest, args.candidate_manifest)
    try:
        baseline = _load_json_file(args.baseline_manifest, side="baseline")
        candidate = _load_json_file(args.candidate_manifest, side="candidate")
        receipt = audit_manifests(baseline, candidate)
        exit_code = 0 if receipt["outcome"] == OUTCOME_EQUIVALENT else 1
    except AuditInputError as exc:
        receipt = _not_computable_receipt(exc)
        exit_code = 2
    try:
        _write_receipt(args.output, receipt, protected_inputs=inputs)
    except AuditInputError as exc:
        print(f"outcome={OUTCOME_NOT_COMPUTABLE} reason_code={exc.reason_code} detail={exc.detail}")
        return 2
    print(
        f"outcome={receipt['outcome']} reason_code={receipt['reason_codes'][0]} "
        f"receipt_sha256={receipt['receipt_sha256']} output={args.output}"
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
