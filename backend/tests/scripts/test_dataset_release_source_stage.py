from __future__ import annotations

import json
import subprocess
import sys
from contextlib import contextmanager
from datetime import date, datetime, UTC
from types import SimpleNamespace

import pytest

from scripts import dataset_release_source_stage as source_stage
from backend.services.dataset_release.source_authority import SourceSnapshotDriftBlocked


def test_source_stage_help_is_a_clean_fresh_process_smoke() -> None:
    completed = subprocess.run(
        [sys.executable, str(source_stage.__file__), "--help"],
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0
    assert "usage: dataset_release_source_stage.py" in completed.stdout
    assert completed.stderr == ""


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


def test_source_stage_freezes_every_partition_in_one_exported_snapshot(monkeypatch) -> None:
    events = []
    identity = SimpleNamespace(
        snapshot_id="00000003-0000001B-1",
        source_as_of=datetime(2026, 9, 21, tzinfo=UTC).isoformat(),
    )

    class Coordinator:
        def __init__(self):
            self.identity = identity

        def assert_no_overlapping_repairs(self):
            events.append("seal")

    @contextmanager
    def coordinator_factory(_connection_factory):
        events.append("open")
        yield Coordinator()
        events.append("close")

    captured = {}
    factory_snapshot_ids = []

    class Authority:
        def freeze(self, **kwargs):
            captured.update(kwargs)
            events.append("freeze")
            return "frozen"

    def authority_factory(_profile, _cas, *, session_factory):
        assert session_factory == "bound-session-factory"
        return Authority()

    def session_factory(snapshot_id, *, connection_factory):
        del connection_factory
        factory_snapshot_ids.append(snapshot_id)
        return "bound-session-factory"

    monkeypatch.setattr(source_stage, "managed_monthly_snapshot", coordinator_factory)
    monkeypatch.setattr(source_stage, "build_source_authority", authority_factory)
    monkeypatch.setattr(source_stage, "imported_source_session_factory", session_factory)

    result = source_stage._freeze_managed_source(
        profile=object(),
        cas=object(),
        cutoff=date(2026, 8, 31),
        checkpoint=lambda: None,
        baseline=(),
        predicted_new_bytes=0,
        disk_checkpoint=lambda _value: None,
        pressure_rung=0,
        sample_instruments=("000001.SZ",),
    )

    assert result == "frozen"
    assert events == ["open", "freeze", "seal", "close"]
    assert factory_snapshot_ids == [identity.snapshot_id]
    assert captured["cutoff"] == date(2026, 8, 31)
    assert captured["sample_instruments"] == ("000001.SZ",)


def test_source_stage_error_envelope_never_persists_raw_exception_text() -> None:
    sensitive_text = "SENSITIVE_VALUE=" + "https://example.invalid/" + "?field=abc"
    envelope = source_stage._sanitized_error_envelope(RuntimeError(sensitive_text))
    encoded = json.dumps(envelope, sort_keys=True)

    assert sensitive_text not in encoded
    assert envelope["exception_type"] == "RuntimeError"
    assert len(envelope["message_sha256"]) == 64
    assert envelope["context_ref"] is None


def test_source_stage_persists_sanitized_typed_error_for_parent(tmp_path) -> None:
    control = (tmp_path / "control").resolve()
    execution = control / "attempt_runs" / "attempt-1-7" / "source-freeze"
    execution.mkdir(parents=True)
    result = execution / "semantic_result.json"
    args = source_stage._parser().parse_args(
        [
            "--profile",
            "profile.yaml",
            "--control-root",
            str(control),
            "--cutoff",
            "2026-07-31",
            "--attempt-id",
            "attempt-1",
            "--attempt-fence",
            "7",
            "--execution-id",
            "source-freeze",
            "--result-path",
            str(result),
            "--predicted-new-bytes",
            "0",
            "--pressure-rung",
            "0",
            "--stage-timeout-seconds",
            "300",
        ]
    )

    source_stage._write_error_result(
        args,
        control_root=control,
        error=SourceSnapshotDriftBlocked(
            "source writer ledger changed during partition freeze",
            context={"query_id": "kline_minute_raw"},
        ),
    )

    payload = json.loads(result.read_text(encoding="utf-8"))
    assert payload["schema_version"] == source_stage.SOURCE_STAGE_ERROR_SCHEMA
    assert payload["error_code"] == "BLOCKED_SOURCE_SNAPSHOT_DRIFT"
    assert payload["exception_type"] == "SourceSnapshotDriftBlocked"
    assert payload["context_ref"] is None
    assert payload["safety"] == source_stage._ZERO_SAFETY
    assert "kline_minute_raw" not in json.dumps(payload, sort_keys=True)


def test_source_stage_main_persists_error_result_before_reraising(tmp_path, monkeypatch) -> None:
    control = (tmp_path / "control").resolve()
    execution = control / "attempt_runs" / "attempt-1-7" / "source-freeze"
    execution.mkdir(parents=True)
    result = execution / "semantic_result.json"
    monkeypatch.setattr(
        source_stage,
        "load_dataset_profile",
        lambda _path: SimpleNamespace(
            control_root=control,
            stage_timeouts_seconds={"source_freeze": 300},
        ),
    )

    def drift(*_args, **_kwargs):
        raise SourceSnapshotDriftBlocked("source writer ledger changed during partition freeze")

    monkeypatch.setattr(source_stage, "_execute_stage", drift)
    argv = [
        "--profile",
        "profile.yaml",
        "--control-root",
        str(control),
        "--cutoff",
        "2026-07-31",
        "--attempt-id",
        "attempt-1",
        "--attempt-fence",
        "7",
        "--execution-id",
        "source-freeze",
        "--result-path",
        str(result),
        "--predicted-new-bytes",
        "0",
        "--pressure-rung",
        "0",
        "--stage-timeout-seconds",
        "300",
    ]

    with pytest.raises(SourceSnapshotDriftBlocked):
        source_stage.main(argv)

    payload = json.loads(result.read_text(encoding="utf-8"))
    assert payload["error_code"] == "BLOCKED_SOURCE_SNAPSHOT_DRIFT"
    assert payload["exception_type"] == "SourceSnapshotDriftBlocked"
    assert payload["safety"] == source_stage._ZERO_SAFETY


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
