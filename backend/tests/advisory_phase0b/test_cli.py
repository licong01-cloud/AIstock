from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.advisory_phase0b.errors import Phase0BAuditError
from backend.tests.advisory_phase0b.test_contracts import _request
from scripts import advisory_phase0b_candidate_quality_audit as cli
from scripts.advisory_phase0b_candidate_quality_audit import (
    _resolve_dataset_root,
    _validate_explicit_identity,
    build_parser,
)


def test_cli_requires_exact_explicit_snapshot_and_target_identity() -> None:
    request = _request()
    target_hash = str(request.audit_targets[0].target_hash)
    _validate_explicit_identity(
        request=request,
        snapshot_ids=request.snapshot_ids,
        target_hashes=(target_hash,),
    )

    with pytest.raises(Phase0BAuditError, match="snapshot ids differ"):
        _validate_explicit_identity(
            request=request,
            snapshot_ids=request.snapshot_ids + request.snapshot_ids,
            target_hashes=(target_hash,),
        )


def test_cli_dataset_root_has_no_relative_or_conflicting_fallback(tmp_path: Path) -> None:
    dataset = (tmp_path / "dataset").resolve()
    other = (tmp_path / "other").resolve()
    dataset.mkdir()
    other.mkdir()

    assert _resolve_dataset_root(
        argument=dataset,
        env_values={"AISTOCK_ADVISORY_DATASET_STORE_ROOT": str(dataset)},
    ) == dataset
    with pytest.raises(Phase0BAuditError, match="differs"):
        _resolve_dataset_root(
            argument=dataset,
            env_values={"AISTOCK_ADVISORY_DATASET_STORE_ROOT": str(other)},
        )
    with pytest.raises(Phase0BAuditError, match="must be absolute"):
        _resolve_dataset_root(argument=Path("relative"), env_values={})


def test_cli_exposes_no_latest_repair_or_write_mode() -> None:
    options = {option for action in build_parser()._actions for option in action.option_strings}
    assert "--latest" not in options
    assert "--repair" not in options
    assert "--write-db" not in options


def test_cli_unexpected_failure_does_not_echo_raw_exception(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    def _unexpected(_path: Path) -> dict[str, str]:
        raise RuntimeError("secret-runtime-text")

    monkeypatch.setattr(cli, "_read_env", _unexpected)
    exit_code = cli.main(
        [
            "--request",
            str(tmp_path / "request.json"),
            "--snapshot-id",
            "snapshot-1",
            "--audit-target-hash",
            "a" * 64,
            "--output-root",
            str(tmp_path.resolve()),
        ]
    )

    stderr = capsys.readouterr().err
    assert exit_code == 3
    assert "secret-runtime-text" not in stderr
    assert '"error_type": "RuntimeError"' in stderr
