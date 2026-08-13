from __future__ import annotations

import json

import pytest

from scripts import dataset_release_source_stage as source_stage


def test_source_stage_accepts_only_explicit_repeated_sample_instruments() -> None:
    arguments = source_stage._parser().parse_args(
        [
            "--profile",
            "profile.yaml",
            "--control-root",
            "X:/control",
            "--cutoff",
            "2026-07-31",
            "--attempt-id",
            "attempt-1",
            "--attempt-fence",
            "1",
            "--execution-id",
            "source-freeze",
            "--result-path",
            "X:/control/result.json",
            "--predicted-new-bytes",
            "0",
            "--pressure-rung",
            "0",
            "--stage-timeout-seconds",
            "300",
            "--sample-instrument",
            "000001.SZ",
            "--sample-instrument",
            "600462.SH",
        ]
    )

    assert arguments.sample_instrument == ["000001.SZ", "600462.SH"]


def test_source_stage_error_envelope_never_persists_raw_exception_text() -> None:
    sensitive_text = "SENSITIVE_VALUE=" + "https://example.invalid/" + "?field=abc"
    envelope = source_stage._sanitized_error_envelope(RuntimeError(sensitive_text))
    encoded = json.dumps(envelope, sort_keys=True)

    assert sensitive_text not in encoded
    assert envelope["exception_type"] == "RuntimeError"
    assert len(envelope["message_sha256"]) == 64
    assert envelope["context_ref"] is None


def test_source_stage_result_is_bound_to_exact_attempt_execution(tmp_path) -> None:
    control = (tmp_path / "control").resolve()
    execution = control / "attempt_runs" / "attempt-1-7" / "source-freeze"
    execution.mkdir(parents=True)
    result = execution / "semantic_result.json"
    payload = {"schema_version": "fixture"}

    source_stage._write_result(
        result,
        payload,
        control_root=control,
        attempt_id="attempt-1",
        attempt_fence=7,
        execution_id="source-freeze",
    )
    assert json.loads(result.read_text(encoding="utf-8")) == payload

    other = control / "attempt_runs" / "attempt-2-7" / "source-freeze"
    other.mkdir(parents=True)
    with pytest.raises(ValueError, match="exact execution root"):
        source_stage._write_result(
            other / "semantic_result.json",
            payload,
            control_root=control,
            attempt_id="attempt-1",
            attempt_fence=7,
            execution_id="source-freeze",
        )


def test_source_stage_result_rejects_symlink_chain_when_supported(tmp_path) -> None:
    control = (tmp_path / "control").resolve()
    real = control / "real"
    real.mkdir(parents=True)
    linked = control / "attempt_runs"
    try:
        linked.symlink_to(real, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation is unavailable")
    execution = linked / "attempt-1-7" / "source-freeze"
    execution.mkdir(parents=True)

    with pytest.raises(ValueError, match="link/reparse"):
        source_stage._write_result(
            execution / "semantic_result.json",
            {"schema_version": "fixture"},
            control_root=control,
            attempt_id="attempt-1",
            attempt_fence=7,
            execution_id="source-freeze",
        )
