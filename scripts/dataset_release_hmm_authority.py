#!/usr/bin/env python
"""Freeze an approved HMM coefficient product for the monthly release worker."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Any, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.dataset_release.canonical import canonical_json_bytes  # noqa: E402
from backend.services.dataset_release.monthly_hmm_authority_bootstrap import (  # noqa: E402
    MonthlyHMMAuthorityBootstrapError,
    MonthlyHMMAuthorityBootstrapResult,
    BOOTSTRAP_RECEIPT_SCHEMA,
    bootstrap_monthly_hmm_authority,
    _plain_path,
    _sha256,
    _read_object,
    _write_exclusive,
    _SHA256,
)
from backend.services.dataset_release.monthly_production import MONTHLY_HMM_AUTHORITY_SCHEMA, load_monthly_hmm_authority  # noqa: E402
from backend.services.dataset_release.monthly_runtime import MonthlyRuntimeConfigurationError  # noqa: E402


def verify_monthly_hmm_authority(*, authority_path: Path, producer_script: Path) -> dict[str, Any]:
    """Run the exact startup loader without processes, data reads, or writes."""

    authority = load_monthly_hmm_authority(authority_path, producer_script=producer_script)
    return {
        "schema_version": "aistock_monthly_hmm_authority_preflight_v1",
        "status": "PASS",
        "authority_path": str(authority_path),
        "authority_file_sha256": _sha256(authority_path),
        "authority_id": authority.authority_id,
        "producer_script_sha256": authority.script_sha256,
        "model_sha256": authority.model_sha256,
        "config_sha256": authority.config_sha256,
        "database_read_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }


def rebind_monthly_hmm_authority(
    *,
    previous_authority: Path,
    expected_previous_authority_sha256: str,
    producer_script: Path,
    expected_producer_sha256: str,
    output_root: Path,
    project_root: Path,
    authority_id: str,
) -> MonthlyHMMAuthorityBootstrapResult:
    """Package an explicitly reviewed producer change; never alter model semantics.

    The two expected hashes are supplied by the delivery owner, not discovered
    as a silent drift repair. The installed package and all approved payloads
    remain immutable. Runtime's strict producer hash validation is unchanged.
    """

    previous = _plain_path(previous_authority, label="previous authority", kind="file")
    script = _plain_path(producer_script, label="HMM coefficient producer", kind="file")
    for digest, path, label in (
        (expected_previous_authority_sha256, previous, "previous authority"),
        (expected_producer_sha256, script, "reviewed producer"),
    ):
        if _SHA256.fullmatch(digest) is None or _sha256(path) != digest:
            raise MonthlyHMMAuthorityBootstrapError(f"{label} SHA256 differs")
    value = _read_object(previous, label="previous authority")
    receipt_path = _plain_path(
        previous.parent / "bootstrap_receipt.json", label="previous bootstrap receipt", kind="file"
    )
    receipt = _read_object(receipt_path, label="previous bootstrap receipt")
    if (
        set(value) != {"schema_version", "authority_id", "model", "config", "producer_script_sha256", "products"}
        or previous.read_bytes() != canonical_json_bytes(value) + b"\n"
        or receipt_path.read_bytes() != canonical_json_bytes(receipt) + b"\n"
        or value.get("schema_version") != MONTHLY_HMM_AUTHORITY_SCHEMA
        or receipt.get("schema_version") != BOOTSTRAP_RECEIPT_SCHEMA
        or receipt.get("authority_file") != previous.name
        or receipt.get("authority_file_sha256") != expected_previous_authority_sha256
        or receipt.get("authority_id") != value.get("authority_id")
        or receipt.get("producer_script_sha256") != value.get("producer_script_sha256")
        or not isinstance(value.get("producer_script_sha256"), str)
        or _SHA256.fullmatch(value["producer_script_sha256"]) is None
        or authority_id == value.get("authority_id")
    ):
        raise MonthlyHMMAuthorityBootstrapError("previous authority/receipt identity differs")
    manifest_sha = receipt.get("expected_dataset_manifest_sha256")
    source_identity = receipt.get("source_dataset_identity")
    if (
        not isinstance(manifest_sha, str)
        or _SHA256.fullmatch(manifest_sha) is None
        or not isinstance(source_identity, dict)
        or source_identity.get("dataset_manifest_sha256") != manifest_sha
    ):
        raise MonthlyHMMAuthorityBootstrapError("previous dataset identity differs")
    products = value.get("products")
    if not isinstance(products, list) or len(products) != 1 or not isinstance(products[0], dict):
        raise MonthlyHMMAuthorityBootstrapError("producer-only rebind requires one approved product")
    product = products[0]
    if set(product) != {"asset_id", "preset_key", "preset_coefficients", "test_start", "backtest_lag_trade_days"}:
        raise MonthlyHMMAuthorityBootstrapError("previous product fields differ")
    for key in ("asset_id", "preset_key", "preset_coefficients", "test_start", "backtest_lag_trade_days"):
        if key not in product or receipt.get(key) != product[key]:
            raise MonthlyHMMAuthorityBootstrapError(f"previous product {key} differs")
    for key in ("model", "config"):
        binding = value.get(key)
        if (
            not isinstance(binding, dict)
            or set(binding) != {"path", "sha256", "size"}
            or receipt.get(f"{key}_sha256") != binding["sha256"]
        ):
            raise MonthlyHMMAuthorityBootstrapError(f"previous {key} binding differs")
        path = _plain_path(Path(str(binding["path"])), label=f"previous {key}", kind="file")
        if (
            path.parent != previous.parent
            or type(binding["size"]) is not int
            or path.stat().st_size != binding["size"]
            or _sha256(path) != binding["sha256"]
        ):
            raise MonthlyHMMAuthorityBootstrapError(f"previous {key} bytes differ")
    source_name = receipt.get("source_coefficient_file")
    if not isinstance(source_name, str) or Path(source_name).name != source_name:
        raise MonthlyHMMAuthorityBootstrapError("previous source coefficient path differs")
    source = _plain_path(previous.parent / source_name, label="approved coefficients", kind="file")
    if _sha256(source) != receipt.get("source_coefficient_sha256"):
        raise MonthlyHMMAuthorityBootstrapError("previous approved coefficient bytes differ")
    source_value = _read_object(source, label="approved coefficients")
    if (
        source_value.get("dataset_identity") != receipt.get("source_dataset_identity")
        or source_value.get("preset_key") != product["preset_key"]
        or source_value.get("preset_coeffs") != product["preset_coefficients"]
        or source_value.get("test_start") != product["test_start"]
        or source_value.get("backtest_end") != receipt.get("backtest_end")
    ):
        raise MonthlyHMMAuthorityBootstrapError("previous approved coefficient semantics differ")
    if _sha256(previous) != expected_previous_authority_sha256:
        raise MonthlyHMMAuthorityBootstrapError("previous authority changed during producer rebind")
    result = bootstrap_monthly_hmm_authority(
        source_coefficients=source,
        model_path=Path(value["model"]["path"]),
        config_path=Path(value["config"]["path"]),
        producer_script=script,
        output_root=output_root,
        project_root=project_root,
        authority_id=authority_id,
        asset_id=product["asset_id"],
        expected_dataset_manifest_sha256=receipt["expected_dataset_manifest_sha256"],
        backtest_lag_trade_days=product["backtest_lag_trade_days"],
    )
    verified = verify_monthly_hmm_authority(authority_path=result.authority_path, producer_script=script)
    if (
        verified["producer_script_sha256"] != expected_producer_sha256
        or verified["model_sha256"] != value["model"]["sha256"]
        or verified["config_sha256"] != value["config"]["sha256"]
        or _sha256(result.root / "approved_coefficients.json") != receipt["source_coefficient_sha256"]
    ):
        raise MonthlyHMMAuthorityBootstrapError("approved identity changed during producer rebind")
    provenance = {
        "schema_version": "aistock_monthly_hmm_producer_rebind_v1",
        "previous_authority_path": str(previous),
        "previous_authority_file_sha256": expected_previous_authority_sha256,
        "previous_bootstrap_receipt_sha256": _sha256(receipt_path),
        "previous_producer_script_sha256": value["producer_script_sha256"],
        "reviewed_producer_script_sha256": expected_producer_sha256,
        "authority_file_sha256": result.authority_sha256,
        "model_sha256": verified["model_sha256"],
        "config_sha256": verified["config_sha256"],
        "source_coefficient_sha256": receipt["source_coefficient_sha256"],
        "model_parameters_changed": False,
        "approved_coefficients_regenerated": False,
    }
    _write_exclusive(result.root / "producer_rebind_receipt.json", canonical_json_bytes(provenance) + b"\n")
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--verify-only", action="store_true")
    modes.add_argument("--rebind-from-authority", type=Path)
    parser.add_argument("--authority-path", type=Path)
    parser.add_argument("--expected-previous-authority-sha256")
    parser.add_argument("--expected-producer-sha256")
    parser.add_argument("--source-coefficients", type=Path)
    parser.add_argument("--model-path", type=Path)
    parser.add_argument("--config-path", type=Path)
    parser.add_argument("--producer-script", type=Path, required=True)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--authority-id")
    parser.add_argument("--asset-id")
    parser.add_argument("--expected-dataset-manifest-sha256")
    parser.add_argument("--backtest-lag-trade-days", type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.verify_only:
            if args.authority_path is None:
                raise MonthlyHMMAuthorityBootstrapError("--verify-only requires --authority-path")
            if any(
                value is not None
                for value in (
                    args.source_coefficients,
                    args.model_path,
                    args.config_path,
                    args.output_root,
                    args.authority_id,
                    args.asset_id,
                    args.expected_dataset_manifest_sha256,
                    args.expected_previous_authority_sha256,
                    args.expected_producer_sha256,
                    args.backtest_lag_trade_days,
                )
            ):
                raise MonthlyHMMAuthorityBootstrapError("read-only verify cannot accept write/rebind options")
            payload = verify_monthly_hmm_authority(
                authority_path=args.authority_path,
                producer_script=args.producer_script,
            )
            sys.stdout.buffer.write(canonical_json_bytes(payload) + b"\n")
            return 0
        if args.rebind_from_authority is not None:
            if any(
                value is None
                for value in (
                    args.expected_previous_authority_sha256,
                    args.expected_producer_sha256,
                    args.output_root,
                    args.authority_id,
                )
            ):
                raise MonthlyHMMAuthorityBootstrapError(
                    "producer rebind requires explicit old/new hashes, output root and authority id"
                )
            if any(
                value is not None
                for value in (
                    args.source_coefficients,
                    args.model_path,
                    args.config_path,
                    args.asset_id,
                    args.expected_dataset_manifest_sha256,
                    args.authority_path,
                    args.backtest_lag_trade_days,
                )
            ):
                raise MonthlyHMMAuthorityBootstrapError("producer-only rebind cannot override approved inputs")
            result = rebind_monthly_hmm_authority(
                previous_authority=args.rebind_from_authority,
                expected_previous_authority_sha256=args.expected_previous_authority_sha256,
                expected_producer_sha256=args.expected_producer_sha256,
                producer_script=args.producer_script,
                output_root=args.output_root,
                project_root=PROJECT_ROOT,
                authority_id=args.authority_id,
            )
        else:
            if any(
                value is not None
                for value in (
                    args.authority_path,
                    args.expected_previous_authority_sha256,
                    args.expected_producer_sha256,
                )
            ):
                raise MonthlyHMMAuthorityBootstrapError("bootstrap cannot accept verify/rebind-only options")
            if any(
                value is None
                for value in (
                    args.source_coefficients,
                    args.model_path,
                    args.output_root,
                    args.authority_id,
                    args.asset_id,
                    args.expected_dataset_manifest_sha256,
                )
            ):
                raise MonthlyHMMAuthorityBootstrapError(
                    "bootstrap requires explicit approved inputs, output root and identity"
                )
            result = bootstrap_monthly_hmm_authority(
                source_coefficients=args.source_coefficients,
                model_path=args.model_path,
                config_path=args.config_path,
                producer_script=args.producer_script,
                output_root=args.output_root,
                project_root=PROJECT_ROOT,
                authority_id=args.authority_id,
                asset_id=args.asset_id,
                expected_dataset_manifest_sha256=(args.expected_dataset_manifest_sha256),
                backtest_lag_trade_days=(1 if args.backtest_lag_trade_days is None else args.backtest_lag_trade_days),
            )
        payload = {
            "schema_version": "aistock_monthly_hmm_authority_bootstrap_cli_v1",
            "status": "PASS",
            "root": str(result.root),
            "authority_path": str(result.authority_path),
            "authority_sha256": result.authority_sha256,
            "receipt_path": str(result.receipt_path),
            "receipt_sha256": result.receipt_sha256,
        }
        sys.stdout.buffer.write(canonical_json_bytes(payload) + b"\n")
        return 0
    except (
        MonthlyHMMAuthorityBootstrapError,
        MonthlyRuntimeConfigurationError,
        OSError,
        ValueError,
        TypeError,
        KeyError,
    ) as exc:
        payload = {
            "schema_version": "aistock_monthly_hmm_authority_bootstrap_cli_v1",
            "status": "FAILED",
            "reason_code": "MONTHLY_HMM_AUTHORITY_BOOTSTRAP_FAILED",
            "message": str(exc),
        }
        sys.stderr.buffer.write(canonical_json_bytes(payload) + b"\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
