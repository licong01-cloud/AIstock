from __future__ import annotations

import sys

import scripts.advisory_price_range_calibration_prepare_request as calibration_cli
import scripts.advisory_price_range_prepare_request as training_cli
from backend.services.advisory_model_first.price_range_bundle import (
    read_daily_price_envelope_bundle_manifest,
)
from backend.services.advisory_model_first.price_range_calibration_contracts import (
    build_frozen_daily_price_envelope_calibration_request,
)
from backend.services.advisory_model_first.price_range_contracts import (
    build_frozen_daily_price_envelope_training_request,
)


def test_training_request_cli_defaults_to_legacy_contract(
    monkeypatch,
) -> None:
    monkeypatch.setattr(sys, "argv", ["request-cli", *_training_args()])

    assert training_cli.parse_args().contract == "legacy-v1"


def test_training_request_cli_accepts_daily_envelope_contract(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "request-cli",
            *_training_args(),
            "--contract", "daily-envelope-v1",
        ],
    )

    assert training_cli.parse_args().contract == "daily-envelope-v1"


def test_calibration_request_cli_defaults_to_legacy_contract(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["request-cli", *_calibration_args()],
    )

    assert calibration_cli.parse_args().contract == "legacy-v1"


def test_calibration_request_cli_accepts_daily_envelope_contract(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["request-cli", *_calibration_args(), "--contract", "daily-envelope-v1"],
    )

    assert calibration_cli.parse_args().contract == "daily-envelope-v1"


def test_daily_envelope_contract_selects_v2_builders_and_v3_parent_reader() -> None:
    assert (
        training_cli._request_builder("daily-envelope-v1")
        is build_frozen_daily_price_envelope_training_request
    )
    reader, builder, parent_schema = calibration_cli._calibration_contract(
        "daily-envelope-v1"
    )

    assert reader is read_daily_price_envelope_bundle_manifest
    assert builder is build_frozen_daily_price_envelope_calibration_request
    assert parent_schema == "advisory_price_range_bundle_v3"


def _training_args() -> list[str]:
    return [
        "--parent-bundle-windows", "parent",
        "--outcome-bundle-windows", "outcome",
        "--parent-run-windows", "run",
        "--repository-root-windows", "repo",
        "--repository-root-wsl", "/repo",
        "--request-output-dir-windows", "requests",
    ]


def _calibration_args() -> list[str]:
    return [
        "--parent-bundle-windows", "parent",
        "--parent-bundle-wsl", "/parent",
        "--features-windows", "features.parquet",
        "--features-wsl", "/features.parquet",
        "--price-range-labels-windows", "labels.parquet",
        "--price-range-labels-wsl", "/labels.parquet",
        "--output-root-wsl", "/output",
        "--repository-root-windows", "repo",
        "--repository-root-wsl", "/repo",
        "--request-output", "request.json",
    ]
